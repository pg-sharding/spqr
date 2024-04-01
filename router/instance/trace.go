package instance

import (
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/pg-sharding/spqr/pkg/config"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-lib/metrics"
)

func (r *InstanceImpl) initJaegerTracer(rcfg *config.Router) (io.Closer, error) {
	cfg := jaegercfg.Configuration{
		ServiceName: "worldmock",
		Sampler: &jaegercfg.SamplerConfig{
			Type:              "const",
			Param:             1,
			SamplingServerURL: rcfg.JaegerUrl,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans: false,
		},
		Gen128Bit: true,
		Tags: []opentracing.Tag{
			{Key: "span.kind", Value: "server"},
		},
	}

	jLogger := jaegerlog.StdLogger //TODO: replace with spqrlog logger
	jMetricsFactory := metrics.NullFactory

	// Initialize tracer with a logger and a metrics factory
	return cfg.InitGlobalTracer(
		"worldmock",
		jaegercfg.Logger(jLogger),
		jaegercfg.Metrics(jMetricsFactory),
	)
}
