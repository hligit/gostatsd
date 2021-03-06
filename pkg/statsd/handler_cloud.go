package statsd

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ash2k/stager/wait"

	"github.com/hligit/gostatsd"
	"github.com/hligit/gostatsd/pkg/stats"
)

// CloudHandler enriches metrics and events with additional information fetched from cloud provider.
type CloudHandler struct {
	// These fields are accessed by any go routine, must use atomic ops
	statsCacheHit  uint64 // Cumulative number of cache hits
	statsCacheMiss uint64 // Cumulative number of cache misses

	// All other stats fields may only be read or written by the main CloudHandler.Run goroutine
	statsMetricItemsQueued uint64 // Absolute number of metrics queued, waiting for a CP to respond
	statsMetricHostsQueued uint64 // Absolute number of IPs waiting for a CP to respond for metrics
	statsEventItemsQueued  uint64 // Absolute number of events queued, waiting for a CP to respond
	statsEventHostsQueued  uint64 // Absolute number of IPs waiting for a CP to respond for events

	cachedInstances gostatsd.CachedInstances
	handler         gostatsd.PipelineHandler
	incomingMetrics chan []*gostatsd.Metric
	incomingEvents  chan *gostatsd.Event

	// emitChan triggers a write of all the current stats when it is given a Statser
	emitChan        chan stats.Statser
	awaitingEvents  map[gostatsd.Source][]*gostatsd.Event
	awaitingMetrics map[gostatsd.Source][]*gostatsd.Metric
	toLookupIPs     []gostatsd.Source
	wg              sync.WaitGroup

	estimatedTags int
}

// NewCloudHandler initialises a new cloud handler.
func NewCloudHandler(cachedInstances gostatsd.CachedInstances, handler gostatsd.PipelineHandler) *CloudHandler {
	return &CloudHandler{
		cachedInstances: cachedInstances,
		handler:         handler,
		incomingMetrics: make(chan []*gostatsd.Metric),
		incomingEvents:  make(chan *gostatsd.Event),
		emitChan:        make(chan stats.Statser),
		awaitingEvents:  make(map[gostatsd.Source][]*gostatsd.Event),
		awaitingMetrics: make(map[gostatsd.Source][]*gostatsd.Metric),
		estimatedTags:   handler.EstimatedTags() + cachedInstances.EstimatedTags(),
	}
}

// EstimatedTags returns a guess for how many tags to pre-allocate
func (ch *CloudHandler) EstimatedTags() int {
	return ch.estimatedTags
}

func (ch *CloudHandler) processMetrics(ctx context.Context, metrics []*gostatsd.Metric) {
	mmToDispatch := gostatsd.NewMetricMap()
	var toHandle []*gostatsd.Metric
	for _, m := range metrics {
		if ch.updateTagsAndHostname(m, m.Source) {
			mmToDispatch.Receive(m)
		} else {
			toHandle = append(toHandle, m)
		}
	}

	if !mmToDispatch.IsEmpty() {
		ch.handler.DispatchMetricMap(ctx, mmToDispatch)
	}

	if len(toHandle) > 0 {
		select {
		case <-ctx.Done():
		case ch.incomingMetrics <- toHandle:
		}
	}
}

// DispatchMetricMap re-dispatches a MetricMap through CloudHandler.processMetrics
// TODO: This is inefficient, and should be handled first class, however that is a major re-factor of
//  the CloudHandler.  It is also recommended to not use a CloudHandler in an http receiver based
//  service, as the IP is not propagated.
func (ch *CloudHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	ch.processMetrics(ctx, mm.AsMetrics())
}

func (ch *CloudHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	if ch.updateTagsAndHostname(e, e.Source) {
		ch.handler.DispatchEvent(ctx, e)
		return
	}
	ch.wg.Add(1) // Increment before sending to the channel
	select {
	case <-ctx.Done():
		ch.wg.Done()
	case ch.incomingEvents <- e:
	}
}

// WaitForEvents waits for all event-dispatching goroutines to finish.
func (ch *CloudHandler) WaitForEvents() {
	ch.wg.Wait()
	ch.handler.WaitForEvents()
}

func (ch *CloudHandler) RunMetrics(ctx context.Context, statser stats.Statser) {
	if me, ok := ch.cachedInstances.(MetricEmitter); ok {
		var wg wait.Group
		defer wg.Wait()
		wg.Start(func() {
			me.RunMetrics(ctx, statser)
		})
	}

	// All the channels are unbuffered, so no CSWs
	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
			ch.scheduleEmit(ctx, statser)
		}
	}
}

// scheduleEmit is used to push a request to the main goroutine requesting metrics
// be emitted.  This is done so we can skip atomic operations on most of our metric
// counters.  In line with the flush notifier, it is fire and forget and won't block
func (ch *CloudHandler) scheduleEmit(ctx context.Context, statser stats.Statser) {
	select {
	case ch.emitChan <- statser:
		// success
	case <-ctx.Done():
		// success-ish
	default:
		// at least we tried
	}
}

func (ch *CloudHandler) emit(statser stats.Statser) {
	// atomic
	statser.Gauge("cloudprovider.cache_hit", float64(atomic.LoadUint64(&ch.statsCacheHit)), nil)
	statser.Gauge("cloudprovider.cache_miss", float64(atomic.LoadUint64(&ch.statsCacheMiss)), nil)
	t := gostatsd.Tags{"type:metric"}
	statser.Gauge("cloudprovider.hosts_queued", float64(ch.statsMetricHostsQueued), t)
	statser.Gauge("cloudprovider.items_queued", float64(ch.statsMetricItemsQueued), t)
	t = gostatsd.Tags{"type:event"}
	statser.Gauge("cloudprovider.hosts_queued", float64(ch.statsEventHostsQueued), t)
	statser.Gauge("cloudprovider.items_queued", float64(ch.statsEventItemsQueued), t)
}

func (ch *CloudHandler) Run(ctx context.Context) {
	var (
		toLookupC  chan<- gostatsd.Source
		toLookupIP gostatsd.Source
	)
	infoSource := ch.cachedInstances.InfoSource()
	ipSink := ch.cachedInstances.IpSink()
	for {
		select {
		case <-ctx.Done():
			return
		case toLookupC <- toLookupIP:
			toLookupIP = gostatsd.UnknownSource // Enable GC
			toLookupC = nil                     // ip has been sent; if there is nothing to send, will block
		case info := <-infoSource:
			ch.handleInstanceInfo(ctx, info)
		case metrics := <-ch.incomingMetrics:
			// Add metrics to awaitingMetrics, accumulate IPs to lookup
			ch.handleIncomingMetrics(metrics)
		case e := <-ch.incomingEvents:
			// Add event to awaitingEvents, accumulate IPs to lookup
			ch.handleIncomingEvent(e)
		case statser := <-ch.emitChan:
			ch.emit(statser)
		}
		if toLookupC == nil && len(ch.toLookupIPs) > 0 {
			last := len(ch.toLookupIPs) - 1
			toLookupIP = ch.toLookupIPs[last]
			ch.toLookupIPs[last] = gostatsd.UnknownSource // Enable GC
			ch.toLookupIPs = ch.toLookupIPs[:last]
			toLookupC = ipSink
		}
	}
}

func (ch *CloudHandler) handleInstanceInfo(ctx context.Context, info gostatsd.InstanceInfo) {
	metrics := ch.awaitingMetrics[info.IP]
	if len(metrics) > 0 {
		delete(ch.awaitingMetrics, info.IP)
		ch.statsMetricItemsQueued -= uint64(len(metrics))
		ch.statsMetricHostsQueued--
		go ch.updateAndDispatchMetrics(ctx, info.Instance, metrics)
	}
	events := ch.awaitingEvents[info.IP]
	if len(events) > 0 {
		delete(ch.awaitingEvents, info.IP)
		ch.statsEventItemsQueued -= uint64(len(events))
		ch.statsEventHostsQueued--
		go ch.updateAndDispatchEvents(ctx, info.Instance, events)
	}
}

func (ch *CloudHandler) handleIncomingMetrics(metrics []*gostatsd.Metric) {
	for _, m := range metrics {
		queue := ch.awaitingMetrics[m.Source]
		ch.awaitingMetrics[m.Source] = append(queue, m)
		if len(queue) == 0 && len(ch.awaitingEvents[m.Source]) == 0 {
			// This is the first metric for that IP in the queue. Need to fetch an Instance for this IP.
			ch.toLookupIPs = append(ch.toLookupIPs, m.Source)
			ch.statsMetricHostsQueued++
		}
	}
	ch.statsMetricItemsQueued += uint64(len(metrics))
}

func (ch *CloudHandler) handleIncomingEvent(e *gostatsd.Event) {
	queue := ch.awaitingEvents[e.Source]
	ch.awaitingEvents[e.Source] = append(queue, e)
	if len(queue) == 0 && len(ch.awaitingMetrics[e.Source]) == 0 {
		// This is the first event for that IP in the queue. Need to fetch an Instance for this IP.
		ch.toLookupIPs = append(ch.toLookupIPs, e.Source)
		ch.statsEventHostsQueued++
	}
	ch.statsEventItemsQueued++
}

func (ch *CloudHandler) updateAndDispatchMetrics(ctx context.Context, instance *gostatsd.Instance, metrics []*gostatsd.Metric) {
	mm := gostatsd.NewMetricMap()
	for _, m := range metrics {
		updateInplace(m, instance)
		mm.Receive(m)
	}
	ch.handler.DispatchMetricMap(ctx, mm)
}

func (ch *CloudHandler) updateAndDispatchEvents(ctx context.Context, instance *gostatsd.Instance, events []*gostatsd.Event) {
	var dispatched int
	defer func() {
		ch.wg.Add(-dispatched)
	}()
	for _, e := range events {
		updateInplace(e, instance)
		dispatched++
		ch.handler.DispatchEvent(ctx, e)
	}
}

func (ch *CloudHandler) updateTagsAndHostname(obj TagChanger, source gostatsd.Source) bool /*is a cache hit*/ {
	instance, cacheHit := ch.getInstance(source)
	if cacheHit {
		updateInplace(obj, instance)
	}
	return cacheHit
}

func (ch *CloudHandler) getInstance(ip gostatsd.Source) (*gostatsd.Instance, bool /*is a cache hit*/) {
	if ip == gostatsd.UnknownSource {
		return nil, true
	}
	instance, cacheHit := ch.cachedInstances.Peek(ip)
	if !cacheHit {
		atomic.AddUint64(&ch.statsCacheMiss, 1)
		return nil, false
	}
	atomic.AddUint64(&ch.statsCacheHit, 1)
	return instance, true
}

func updateInplace(obj TagChanger, instance *gostatsd.Instance) {
	if instance != nil { // It was a positive cache hit (successful lookup cache, not failed lookup cache)
		obj.AddTagsSetSource(instance.Tags, instance.ID)
	}
}
