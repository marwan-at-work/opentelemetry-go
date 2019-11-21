package datadog

import (
	"context"
	"fmt"

	"github.com/DataDog/datadog-go/statsd"
	export "go.opentelemetry.io/otel/sdk/export/metric"
	"go.opentelemetry.io/otel/sdk/export/metric/aggregator"
	"go.opentelemetry.io/otel/sdk/metric/aggregator/array"
	"go.opentelemetry.io/otel/sdk/metric/aggregator/counter"
	"go.opentelemetry.io/otel/sdk/metric/aggregator/ddsketch"
	"go.opentelemetry.io/otel/sdk/metric/aggregator/gauge"
)

// Exporter is the datadog stats exporter
type Exporter struct {
	client  *statsd.Client
	options Options
}

var _ export.Exporter = &Exporter{}

// Options are the options to be used when initializing a stdout export.
type Options struct {
	// Namespace specifies the namespaces to which metric keys are appended.
	Namespace string

	// Service specifies the service name used for tracing.
	Service string

	// TraceAddr specifies the host[:port] address of the Datadog Trace Agent.
	// It defaults to localhost:8126.
	TraceAddr string

	// StatsAddr specifies the host[:port] address for DogStatsD. It defaults
	// to localhost:8125.
	StatsAddr string

	// OnError specifies a function that will be called if an error occurs during
	// processing stats or metrics.
	OnError func(err error)

	// Tags specifies a set of global tags to attach to each metric.
	Tags []string

	// GlobalTags holds a set of tags that will automatically be applied to all
	// exported spans.
	GlobalTags map[string]interface{}
}

const (
	// DefaultStatsAddrUDP specifies the default protocol (UDP) and address
	// for the DogStatsD service.
	DefaultStatsAddrUDP = "localhost:8125"

	// DefaultStatsAddrUDS specifies the default socket address for the
	// DogStatsD service over UDS. Only useful for platforms supporting unix
	// sockets.
	DefaultStatsAddrUDS = "unix:///var/run/datadog/dsd.socket"
)

// New returns a new exporter
func New(options Options) (*Exporter, error) {
	endpoint := options.StatsAddr
	if endpoint == "" {
		endpoint = DefaultStatsAddrUDP
	}

	c, err := statsd.New(endpoint)
	if err != nil {
		return nil, err
	}
	return &Exporter{
		client:  c,
		options: options,
	}, nil
}

// Export implements the Exporter interface
func (e *Exporter) Export(_ context.Context, checkpointSet export.CheckpointSet) error {
	var firstErr error
	checkpointSet.ForEach(func(record export.Record) {
		agg := record.Aggregator()
		metricName := e.withNamespace(record.Descriptor().Name())

		tags := append([]string{}, e.options.Tags...)
		for _, label := range record.Labels().Ordered() {
			tags = append(tags, string(label.Key)+":"+label.Value.Emit())
		}
		var err error
		switch agg := agg.(type) {
		case *counter.Aggregator:
			count, _ := agg.Sum()
			err = e.client.Count(metricName, int64(count), tags, 1)
		case *array.Aggregator:
			err = e.sendArray(metricName, tags, agg)
		case *ddsketch.Aggregator:
			err = e.sendArray(metricName, tags, agg)
		case *gauge.Aggregator:
			val, _, _ := agg.LastValue()
			err = e.client.Gauge(metricName, float64(val), tags, 1)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	})
	return firstErr
}

func (e *Exporter) sendArray(metricName string, tags []string, agg aggregator.Distribution) error {
	count, err := agg.Count()
	if err != nil {
		return err
	}
	err = e.client.Count(metricName+".count", count, tags, 1)
	if err != nil {
		return err
	}
	min, err := agg.Quantile(0)
	if err != nil && err != aggregator.ErrEmptyDataSet {
		return err
	}
	if err != aggregator.ErrEmptyDataSet {
		err = e.client.Gauge(metricName+".min", float64(min), tags, 1)
		if err != nil {
			return err
		}
	}
	max, err := agg.Max()
	if err != nil && err != aggregator.ErrEmptyDataSet {
		return err
	}
	if err != aggregator.ErrEmptyDataSet {
		err = e.client.Gauge(metricName+".max", float64(max), tags, 1)
		if err != nil {
			return err
		}
	}
	avg, err := agg.Quantile(0.5)
	if err != nil && err != aggregator.ErrEmptyDataSet {
		return err
	}
	if err != aggregator.ErrEmptyDataSet {
		err = e.client.Gauge(metricName+".avg", float64(avg), tags, 1)
		if err != nil {
			return err
		}
	}
	ninefive, err := agg.Quantile(0.95)
	if err != nil && err != aggregator.ErrEmptyDataSet {
		return err
	}
	if err != aggregator.ErrEmptyDataSet {
		e.client.Gauge(metricName+".95th_percentile", float64(ninefive), tags, 1)
		if err != nil {
			return err
		}
	}
	fmt.Println("MIN", float64(min), "AVG", float64(avg), "95", float64(ninefive), "MAX", float64(max))
	return nil
}

func (e *Exporter) withNamespace(metricName string) string {
	nameSpace := e.options.Namespace
	if nameSpace == "" {
		return metricName
	}
	return nameSpace + "_" + metricName
}
