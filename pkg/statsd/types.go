package statsd

import (
	"context"
	"time"

	"github.com/hligit/gostatsd"
	"github.com/hligit/gostatsd/pkg/stats"
)

// DispatcherProcessFunc is a function that gets executed by Dispatcher for each Aggregator, passing it into the function.
type DispatcherProcessFunc func(int, Aggregator)

// AggregateProcesser is an interface to run a function against each Aggregator, in the goroutine
// context of that Aggregator.
type AggregateProcesser interface {
	Process(ctx context.Context, fn DispatcherProcessFunc) gostatsd.Wait
}

// ProcessFunc is a function that gets executed by Aggregator with its state passed into the function.
type ProcessFunc func(*gostatsd.MetricMap)

// Aggregator is an object that aggregates statsd metrics.
// The function NewAggregator should be used to create the objects.
//
// Incoming metrics should be passed via ReceiveMap function.
type Aggregator interface {
	ReceiveMap(mm *gostatsd.MetricMap)
	Flush(interval time.Duration)
	Process(ProcessFunc)
	Reset()
}

// Datagram is a received UDP datagram that has not been parsed into Metric/Event(s)
type Datagram struct {
	IP        gostatsd.Source
	Msg       []byte
	Timestamp gostatsd.Nanotime
	DoneFunc  func() // to be called once the datagram has been parsed and msg can be freed
}

// MetricEmitter is an object that emits metrics.  Used to pass a Statser to the object
// after initialization, as Statsers may be created after MetricEmitters
type MetricEmitter interface {
	RunMetrics(ctx context.Context, statser stats.Statser)
}

// TagChanger is an interface that Metric/Event can implement to update their tags
// and source.  It is so the CloudHandler can change the tags without worrying about
// the TagsKey cache.
type TagChanger interface {
	AddTagsSetSource(additionalTags gostatsd.Tags, newSource gostatsd.Source)
}
