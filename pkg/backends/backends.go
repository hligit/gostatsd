package backends

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/hligit/gostatsd"
	"github.com/hligit/gostatsd/pkg/backends/cloudwatch"
	"github.com/hligit/gostatsd/pkg/backends/datadog"
	"github.com/hligit/gostatsd/pkg/backends/graphite"
	"github.com/hligit/gostatsd/pkg/backends/influxdb"
	"github.com/hligit/gostatsd/pkg/backends/newrelic"
	"github.com/hligit/gostatsd/pkg/backends/null"
	"github.com/hligit/gostatsd/pkg/backends/statsdaemon"
	"github.com/hligit/gostatsd/pkg/backends/stdout"
	"github.com/hligit/gostatsd/pkg/transport"
)

// All known backends.
var backends = map[string]gostatsd.BackendFactory{
	datadog.BackendName:     datadog.NewClientFromViper,
	graphite.BackendName:    graphite.NewClientFromViper,
	influxdb.BackendName:    influxdb.NewClientFromViper,
	null.BackendName:        null.NewClientFromViper,
	statsdaemon.BackendName: statsdaemon.NewClientFromViper,
	stdout.BackendName:      stdout.NewClientFromViper,
	cloudwatch.BackendName:  cloudwatch.NewClientFromViper,
	newrelic.BackendName:    newrelic.NewClientFromViper,
}

// GetBackend creates an instance of the named backend, or nil if
// the name is not known. The error return is only used if the named backend
// was known but failed to initialize.
func GetBackend(name string, v *viper.Viper, logger logrus.FieldLogger, pool *transport.TransportPool) (gostatsd.Backend, error) {
	f, found := backends[name]
	if !found {
		return nil, nil
	}
	return f(v, logger, pool)
}

// InitBackend creates an instance of the named backend.
func InitBackend(name string, v *viper.Viper, logger logrus.FieldLogger, pool *transport.TransportPool) (gostatsd.Backend, error) {
	if name == "" {
		logger.Info("No backend specified")
		return nil, nil
	}

	logger = logger.WithField("backend", name)

	backend, err := GetBackend(name, v, logger, pool)
	if err != nil {
		return nil, fmt.Errorf("could not init backend %q: %v", name, err)
	}
	if backend == nil {
		return nil, fmt.Errorf("unknown backend %q", name)
	}
	logger.Info("Initialised backend")

	return backend, nil
}
