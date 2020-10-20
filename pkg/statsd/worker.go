package statsd

import (
	"context"
	"fmt"
	"time"

	"github.com/ash2k/stager/wait"

	"github.com/hligit/gostatsd"
	"github.com/hligit/gostatsd/pkg/stats"
)

type processCommand struct {
	f    DispatcherProcessFunc
	done func()
}

type worker struct {
	aggr           Aggregator
	metricMapQueue chan *gostatsd.MetricMap
	processChan    chan *processCommand
	id             int
}

func (w *worker) work() {
	for {
		select {
		case mm, ok := <-w.metricMapQueue:
			if !ok {
				return
			}
			w.aggr.ReceiveMap(mm)
		case cmd := <-w.processChan:
			w.executeProcess(cmd)
		}
	}
}

func (w *worker) executeProcess(cmd *processCommand) {
	defer cmd.done() // Done with the process command
	cmd.f(w.id, w.aggr)
}

func (w *worker) RunMetrics(ctx context.Context, statser stats.Statser) {
	wg := &wait.Group{}
	wg.StartWithContext(ctx, stats.NewChannelStatsWatcher(
		statser,
		"dispatch_aggregator_map",
		gostatsd.Tags{fmt.Sprintf("aggregator_id:%d", w.id)},
		cap(w.metricMapQueue),
		func() int { return len(w.metricMapQueue) },
		1000*time.Millisecond,
	).Run)
	wg.Wait()
}
