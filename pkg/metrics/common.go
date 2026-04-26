package metrics

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	MaxMetricStartupTime = 5 * time.Second
	MetricStartupTick    = 100 * time.Millisecond

	// prefix of all metric names.
	DefaultMetricPath = "/metric"
	MetricPrefix      = "spqr_"
)

type MetricRegistry interface {
	GetRegistry() *prometheus.Registry
}

func Start(registry MetricRegistry, path string, port string) error {
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum < 1 || portNum > math.MaxUint16 {
		return fmt.Errorf("port for metric exporter have incorrect value: %s", port)
	}
	metricsMux := http.NewServeMux()
	metricsMux.Handle(path, promhttp.HandlerFor(registry.GetRegistry(), promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

	metricsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", portNum),
		Handler: metricsMux,
	}

	errChan := make(chan error, 1)

	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), MaxMetricStartupTime)
	defer cancel()

	ticker := time.NewTicker(MetricStartupTick)
	defer ticker.Stop()

	for {
		select {
		case err := <-errChan:
			return fmt.Errorf("metrics server startup failed: %w", err)
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for metrics server to start")
		case <-ticker.C:
			dialCtx, dialCancel := context.WithTimeout(ctx, MetricStartupTick)

			var dialer net.Dialer
			conn, err := dialer.DialContext(dialCtx, "tcp", fmt.Sprintf("localhost:%d", portNum))
			dialCancel()

			if err == nil {
				_ = conn.Close()
				return nil
			}
		}
	}
}

type DynamicGauge struct {
	Name   string
	Help   string
	Getter func() float64
	Value  float64
}

func (g *DynamicGauge) Desc() *prometheus.Desc {
	return prometheus.NewDesc(g.Name, g.Help, nil, nil)
}

func (g *DynamicGauge) Collect(ch chan<- prometheus.Metric) {
	value := g.Getter()
	ch <- prometheus.MustNewConstMetric(g.Desc(), prometheus.GaugeValue, value)
}

func (g *DynamicGauge) Describe(ch chan<- *prometheus.Desc) {
	ch <- g.Desc()
}
