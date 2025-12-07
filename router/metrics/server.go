package metrics

import (
	"net/http"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartMetricsServer starts HTTP server for Prometheus metrics
func StartMetricsServer(port string) {
	mux := http.NewServeMux()

	// Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Info endpoint
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<!DOCTYPE html>
<html>
<head><title>SPQR Metrics</title></head>
<body>
<h1>SPQR Router Metrics</h1>
<ul>
  <li><a href="/metrics">Prometheus Metrics</a></li>
  <li><a href="/health">Health Check</a></li>
</ul>
</body>
</html>`))
	})

	addr := ":" + port
	spqrlog.Zero.Info().
		Str("addr", addr).
		Msg("Starting metrics server")

	// Run in background
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			spqrlog.Zero.Error().
				Err(err).
				Msg("Metrics server failed")
		}
	}()
}
