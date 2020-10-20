package datadog

import (
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/tilinna/clock"

	"github.com/hligit/gostatsd"
	"github.com/hligit/gostatsd/internal/util"
	"github.com/hligit/gostatsd/pkg/stats"
	"github.com/hligit/gostatsd/pkg/transport"
)

const (
	apiURL = "https://app.datadoghq.com"
	// BackendName is the name of this backend.
	BackendName                  = "datadog"
	dogstatsdVersion             = "5.6.3"
	defaultUserAgent             = "gostatsd"
	defaultMaxRequestElapsedTime = 15 * time.Second
	// defaultMetricsPerBatch is the default number of metrics to send in a single batch.
	defaultMetricsPerBatch = 1000
	// maxResponseSize is the maximum response size we are willing to read.
	maxResponseSize     = 1024
	maxConcurrentEvents = 20
)

var (
	// defaultMaxRequests is the number of parallel outgoing requests to Datadog.  As this mixes both
	// CPU (JSON encoding, TLS) and network bound operations, balancing may require some experimentation.
	defaultMaxRequests = uint(2 * runtime.NumCPU())

	// It already does not sort map keys by default, but it does HTML escaping which we don't need.
	jsonConfig = jsoniter.Config{
		EscapeHTML:  false,
		SortMapKeys: false,
	}.Froze()
)

// Client represents a Datadog client.
type Client struct {
	batchesCreated uint64            // Accumulated number of batches created
	batchesDropped uint64            // Accumulated number of batches aborted (data loss)
	batchesSent    uint64            // Accumulated number of batches successfully sent
	seriesSent     uint64            // Accumulated number of series successfully sent
	batchesRetried stats.ChangeGauge // Accumulated number of batches retried (first send is not a retry)

	logger                logrus.FieldLogger
	apiKey                string
	apiEndpoint           string
	userAgent             string
	maxRequestElapsedTime time.Duration
	client                *http.Client
	metricsPerBatch       uint
	metricsBufferSem      chan *bytes.Buffer // Two in one - a semaphore and a buffer pool
	eventsBufferSem       chan *bytes.Buffer // Two in one - a semaphore and a buffer pool
	compressPayload       bool

	disabledSubtypes gostatsd.TimerSubtypes
	flushInterval    time.Duration
}

// event represents an event data structure for Datadog.
type event struct {
	Title          string   `json:"title"`
	Text           string   `json:"text"`
	DateHappened   int64    `json:"date_happened,omitempty"`
	Hostname       string   `json:"host,omitempty"`
	AggregationKey string   `json:"aggregation_key,omitempty"`
	SourceTypeName string   `json:"source_type_name,omitempty"`
	Tags           []string `json:"tags,omitempty"`
	Priority       string   `json:"priority,omitempty"`
	AlertType      string   `json:"alert_type,omitempty"`
}

// SendMetricsAsync flushes the metrics to Datadog, preparing payload synchronously but doing the send asynchronously.
func (d *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	counter := 0
	results := make(chan error)

	now := float64(clock.FromContext(ctx).Now().Unix())
	d.processMetrics(now, metrics, func(ts *timeSeries) {
		// This section would be likely be better if it pushed all ts's in to a single channel
		// which n goroutines then read from.  Current behavior still spins up many goroutines
		// and has them all hit the same channel.
		atomic.AddUint64(&d.batchesCreated, 1)
		go func() {
			select {
			case <-ctx.Done():
				return
			case buffer := <-d.metricsBufferSem:
				defer func() {
					buffer.Reset()
					d.metricsBufferSem <- buffer
				}()
				err := d.postMetrics(ctx, buffer, ts)

				select {
				case <-ctx.Done():
				case results <- err:
				}
			}
		}()
		counter++
	})
	go func() {
		errs := make([]error, 0, counter)
	loop:
		for c := 0; c < counter; c++ {
			select {
			case <-ctx.Done():
				errs = append(errs, ctx.Err())
				break loop
			case err := <-results:
				errs = append(errs, err)
			}
		}
		cb(errs)
	}()
}

func (d *Client) Run(ctx context.Context) {
	statser := stats.FromContext(ctx).WithTags(gostatsd.Tags{"backend:datadog"})

	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
			statser.Gauge("backend.created", float64(atomic.LoadUint64(&d.batchesCreated)), nil)
			d.batchesRetried.SendIfChanged(statser, "backend.retried", nil)
			statser.Gauge("backend.dropped", float64(atomic.LoadUint64(&d.batchesDropped)), nil)
			statser.Gauge("backend.sent", float64(atomic.LoadUint64(&d.batchesSent)), nil)
			statser.Gauge("backend.series.sent", float64(atomic.LoadUint64(&d.seriesSent)), nil)
		}
	}
}

func (d *Client) processMetrics(now float64, metrics *gostatsd.MetricMap, cb func(*timeSeries)) {
	fl := flush{
		ts: &timeSeries{
			Series: make([]metric, 0, d.metricsPerBatch),
		},
		timestamp:        now,
		flushIntervalSec: d.flushInterval.Seconds(),
		metricsPerBatch:  d.metricsPerBatch,
		cb:               cb,
	}

	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		fl.addMetric(rate, counter.PerSecond, counter.Source, counter.Tags, key)
		fl.addMetricf(gauge, float64(counter.Value), counter.Source, counter.Tags, "%s.count", key)
		fl.maybeFlush()
	})

	metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		if timer.Histogram != nil {
			for histogramThreshold, count := range timer.Histogram {
				bucketTag := "le:+Inf"
				if !math.IsInf(float64(histogramThreshold), 1) {
					bucketTag = "le:" + strconv.FormatFloat(float64(histogramThreshold), 'f', -1, 64)
				}
				newTags := timer.Tags.Concat(gostatsd.Tags{bucketTag})
				fl.addMetricf(counter, float64(count), timer.Source, newTags, "%s.histogram", key)
			}
		} else {

			if !d.disabledSubtypes.Lower {
				fl.addMetricf(gauge, timer.Min, timer.Source, timer.Tags, "%s.lower", key)
			}
			if !d.disabledSubtypes.Upper {
				fl.addMetricf(gauge, timer.Max, timer.Source, timer.Tags, "%s.upper", key)
			}
			if !d.disabledSubtypes.Count {
				fl.addMetricf(gauge, float64(timer.Count), timer.Source, timer.Tags, "%s.count", key)
			}
			if !d.disabledSubtypes.CountPerSecond {
				fl.addMetricf(rate, timer.PerSecond, timer.Source, timer.Tags, "%s.count_ps", key)
			}
			if !d.disabledSubtypes.Mean {
				fl.addMetricf(gauge, timer.Mean, timer.Source, timer.Tags, "%s.mean", key)
			}
			if !d.disabledSubtypes.Median {
				fl.addMetricf(gauge, timer.Median, timer.Source, timer.Tags, "%s.median", key)
			}
			if !d.disabledSubtypes.StdDev {
				fl.addMetricf(gauge, timer.StdDev, timer.Source, timer.Tags, "%s.std", key)
			}
			if !d.disabledSubtypes.Sum {
				fl.addMetricf(gauge, timer.Sum, timer.Source, timer.Tags, "%s.sum", key)
			}
			if !d.disabledSubtypes.SumSquares {
				fl.addMetricf(gauge, timer.SumSquares, timer.Source, timer.Tags, "%s.sum_squares", key)
			}
			for _, pct := range timer.Percentiles {
				fl.addMetricf(gauge, pct.Float, timer.Source, timer.Tags, "%s.%s", key, pct.Str)
			}
		}
		fl.maybeFlush()
	})

	metrics.Gauges.Each(func(key, tagsKey string, g gostatsd.Gauge) {
		fl.addMetric(gauge, g.Value, g.Source, g.Tags, key)
		fl.maybeFlush()
	})

	metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		fl.addMetric(gauge, float64(len(set.Values)), set.Source, set.Tags, key)
		fl.maybeFlush()
	})

	fl.finish()
}

func (d *Client) postMetrics(ctx context.Context, buffer *bytes.Buffer, ts *timeSeries) error {
	if err := d.post(ctx, buffer, "/api/v1/series", "metrics", ts); err != nil {
		return err
	}
	atomic.AddUint64(&d.seriesSent, uint64(len(ts.Series)))
	return nil
}

// SendEvent sends an event to Datadog.
func (d *Client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case buffer := <-d.eventsBufferSem:
		defer func() {
			buffer.Reset()
			d.eventsBufferSem <- buffer
		}()
		return d.post(ctx, buffer, "/api/v1/events", "events", &event{
			Title:          e.Title,
			Text:           e.Text,
			DateHappened:   e.DateHappened,
			Hostname:       string(e.Source),
			AggregationKey: e.AggregationKey,
			SourceTypeName: e.SourceTypeName,
			Tags:           e.Tags,
			Priority:       e.Priority.StringWithEmptyDefault(),
			AlertType:      e.AlertType.StringWithEmptyDefault(),
		})
	}
}

// Name returns the name of the backend.
func (d *Client) Name() string {
	return BackendName
}

func (d *Client) post(ctx context.Context, buffer *bytes.Buffer, path, typeOfPost string, data interface{}) error {
	post, err := d.constructPost(ctx, buffer, path, typeOfPost, data)
	if err != nil {
		atomic.AddUint64(&d.batchesDropped, 1)
		return err
	}

	b := backoff.NewExponentialBackOff()
	clck := clock.FromContext(ctx)
	b.Clock = clck
	b.Reset()
	b.MaxElapsedTime = d.maxRequestElapsedTime
	for {
		if err = post(); err == nil {
			atomic.AddUint64(&d.batchesSent, 1)
			return nil
		}

		next := b.NextBackOff()
		if next == backoff.Stop {
			atomic.AddUint64(&d.batchesDropped, 1)
			return fmt.Errorf("[%s] %v", BackendName, err)
		}

		d.logger.WithFields(logrus.Fields{
			"type":  typeOfPost,
			"sleep": next,
			"error": err,
		}).Warn("failed to send")

		timer := clck.NewTimer(next)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		atomic.AddUint64(&d.batchesRetried.Cur, 1)
	}
}

func (d *Client) constructPost(ctx context.Context, buffer *bytes.Buffer, path, typeOfPost string, data interface{}) (func() error /*doPost*/, error) {
	authenticatedURL := d.authenticatedURL(path)
	// Selectively compress payload based on knowledge of whether the endpoint supports deflate encoding.
	// The metrics endpoint does, the events endpoint does not.
	compressPayload := d.compressPayload && typeOfPost == "metrics"
	marshal := func(w io.Writer) error {
		stream := jsonConfig.BorrowStream(w)
		defer jsonConfig.ReturnStream(stream)
		stream.WriteVal(data)
		return stream.Flush()
	}
	var err error
	if compressPayload {
		err = deflate(buffer, marshal)
	} else {
		err = marshal(buffer)
	}
	if err != nil {
		return nil, fmt.Errorf("[%s] unable to marshal %s: %v", BackendName, typeOfPost, err)
	}
	body := buffer.Bytes()

	return func() error {
		headers := map[string]string{
			"Content-Type":         "application/json",
			"DD-Dogstatsd-Version": dogstatsdVersion,
			"User-Agent":           d.userAgent,
		}
		if compressPayload {
			headers["Content-Encoding"] = "deflate"
		}
		req, err := http.NewRequest("POST", authenticatedURL, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("unable to create http.Request: %v", err)
		}
		req = req.WithContext(ctx)
		for header, v := range headers {
			req.Header.Set(header, v)
		}
		resp, err := d.client.Do(req)
		if err != nil {
			return fmt.Errorf("error POSTing: %s", strings.Replace(err.Error(), d.apiKey, "*****", -1))
		}
		defer resp.Body.Close()
		body := io.LimitReader(resp.Body, maxResponseSize)
		if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusNoContent {
			b, _ := ioutil.ReadAll(body)
			d.logger.WithFields(logrus.Fields{
				"status": resp.StatusCode,
				"body":   string(b),
			}).Info("request failed")
			return fmt.Errorf("received bad status code %d", resp.StatusCode)
		}
		_, _ = io.Copy(ioutil.Discard, body)
		return nil
	}, nil
}

func (d *Client) authenticatedURL(path string) string {
	q := url.Values{
		"api_key": []string{d.apiKey},
	}
	return fmt.Sprintf("%s%s?%s", d.apiEndpoint, path, q.Encode())
}

// NewClientFromViper returns a new Datadog API client.
func NewClientFromViper(v *viper.Viper, logger logrus.FieldLogger, pool *transport.TransportPool) (gostatsd.Backend, error) {
	dd := util.GetSubViper(v, "datadog")
	dd.SetDefault("api_endpoint", apiURL)
	dd.SetDefault("metrics_per_batch", defaultMetricsPerBatch)
	dd.SetDefault("compress_payload", true)
	dd.SetDefault("max_request_elapsed_time", defaultMaxRequestElapsedTime)
	dd.SetDefault("max_requests", defaultMaxRequests)
	dd.SetDefault("user-agent", defaultUserAgent)
	dd.SetDefault("transport", "default")

	return NewClient(
		dd.GetString("api_endpoint"),
		dd.GetString("api_key"),
		dd.GetString("user-agent"),
		dd.GetString("transport"),
		dd.GetInt("metrics_per_batch"),
		uint(dd.GetInt("max_requests")),
		dd.GetBool("compress_payload"),
		dd.GetDuration("max_request_elapsed_time"),
		v.GetDuration("flush-interval"), // Main viper, not sub-viper
		gostatsd.DisabledSubMetrics(v),
		logger,
		pool,
	)
}

// NewClient returns a new Datadog API client.
func NewClient(
	apiEndpoint,
	apiKey,
	userAgent,
	transport string,
	metricsPerBatch int,
	maxRequests uint,
	compressPayload bool,
	maxRequestElapsedTime,
	flushInterval time.Duration,
	disabled gostatsd.TimerSubtypes,
	logger logrus.FieldLogger,
	pool *transport.TransportPool,
) (*Client, error) {
	if apiEndpoint == "" {
		return nil, fmt.Errorf("[%s] apiEndpoint is required", BackendName)
	}
	if apiKey == "" {
		return nil, fmt.Errorf("[%s] apiKey is required", BackendName)
	}
	if userAgent == "" {
		return nil, fmt.Errorf("[%s] user-agent is required", BackendName)
	}
	if metricsPerBatch <= 0 {
		return nil, fmt.Errorf("[%s] metricsPerBatch must be positive", BackendName)
	}
	if maxRequestElapsedTime <= 0 && maxRequestElapsedTime != -1 {
		return nil, fmt.Errorf("[%s] maxRequestElapsedTime must be positive", BackendName)
	}

	httpClient, err := pool.Get(transport)
	if err != nil {
		logger.WithError(err).Error("failed to create http client")
		return nil, err
	}
	logger.WithFields(logrus.Fields{
		"max-request-elapsed-time": maxRequestElapsedTime,
		"max-requests":             maxRequests,
		"metrics-per-batch":        metricsPerBatch,
		"compress-payload":         compressPayload,
	}).Info("created backend")

	metricsBufferSem := make(chan *bytes.Buffer, maxRequests)
	for i := uint(0); i < maxRequests; i++ {
		metricsBufferSem <- &bytes.Buffer{}
	}
	eventsBufferSem := make(chan *bytes.Buffer, maxConcurrentEvents)
	for i := uint(0); i < maxConcurrentEvents; i++ {
		eventsBufferSem <- &bytes.Buffer{}
	}
	return &Client{
		logger:                logger,
		apiKey:                apiKey,
		apiEndpoint:           apiEndpoint,
		userAgent:             userAgent,
		maxRequestElapsedTime: maxRequestElapsedTime,
		client:                httpClient.Client,
		metricsPerBatch:       uint(metricsPerBatch),
		metricsBufferSem:      metricsBufferSem,
		eventsBufferSem:       eventsBufferSem,
		compressPayload:       compressPayload,
		flushInterval:         flushInterval,
		disabledSubtypes:      disabled,
	}, nil
}

func deflate(w io.Writer, f func(io.Writer) error) error {
	compressor, err := zlib.NewWriterLevel(w, zlib.BestCompression)
	if err != nil {
		return fmt.Errorf("unable to create zlib writer: %v", err)
	}
	err = f(compressor)
	if err != nil {
		return fmt.Errorf("unable to write compressed payload: %v", err)
	}
	err = compressor.Close()
	if err != nil {
		return fmt.Errorf("unable to close compressor: %v", err)
	}
	return nil
}
