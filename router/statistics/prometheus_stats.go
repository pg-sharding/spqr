package statistics

import (
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// StatisticsType defines the type of statistics being recorded
type StatisticsType string

const (
	StatisticsTypeRouter = StatisticsType("router")
	StatisticsTypeShard  = StatisticsType("shard")
)

// StartTimes holds the start times for router and shard operations
type StartTimes struct {
	RouterStart time.Time
	ShardStart  time.Time
}

// Legacy compatibility: QueryStatistics struct (simplified for Prometheus)
type Statistics struct {
	Quantiles         []float64
	QuantilesStr      []string
	NeedToCollectData bool
}

var QueryStatistics = Statistics{}

var (
	// Query duration histograms (lock-free!)
	routerDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "spqr_router_query_duration_seconds",
		Help: "Router query duration in seconds (end-to-end)",
		Buckets: []float64{
			0.0001, // 100µs
			0.0005, // 500µs
			0.001,  // 1ms
			0.005,  // 5ms
			0.01,   // 10ms
			0.05,   // 50ms
			0.1,    // 100ms
			0.5,    // 500ms
			1.0,    // 1s
			5.0,    // 5s
			10.0,   // 10s
		},
	})

	shardDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "spqr_shard_query_duration_seconds",
		Help: "Shard query duration in seconds (backend execution)",
		Buckets: []float64{
			0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0,
		},
	})

	// Query counters
	queryTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spqr_queries_total",
		Help: "Total number of queries processed",
	})

	// Active connections gauge
	activeConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "spqr_active_connections",
		Help: "Number of active client connections",
	})
)

// InitStatistics initializes statistics with quantiles (legacy compatibility)
func InitStatistics(q []float64) {
	QueryStatistics.Quantiles = q
	QueryStatistics.NeedToCollectData = len(q) > 0
}

// InitStatisticsStr initializes statistics with string quantiles (legacy compatibility)
func InitStatisticsStr(q []string) error {
	QueryStatistics.QuantilesStr = q
	QueryStatistics.Quantiles = make([]float64, len(q))
	for i, qStr := range q {
		var err error
		QueryStatistics.Quantiles[i], err = strconv.ParseFloat(qStr, 64)
		if err != nil {
			return fmt.Errorf("could not parse time quantile to float: \"%s\"", qStr)
		}
	}
	QueryStatistics.NeedToCollectData = len(q) > 0
	return nil
}

// GetQuantiles returns configured quantiles (legacy compatibility)
func GetQuantiles() *[]float64 {
	return &QueryStatistics.Quantiles
}

// GetQuantilesStr returns configured quantiles as strings (legacy compatibility)
func GetQuantilesStr() *[]string {
	return &QueryStatistics.QuantilesStr
}

// GetTimeQuantile returns 0 - use Prometheus /metrics endpoint instead
// Legacy compatibility: keeps old code compiling but doesn't provide data
func GetTimeQuantile(statType StatisticsType, q float64, h StatHolder) float64 {
	// Prometheus doesn't support per-client quantiles easily
	// Users should query /metrics endpoint instead
	return 0
}

// GetTotalTimeQuantile returns 0 - use Prometheus /metrics endpoint instead
// Legacy compatibility: keeps old code compiling but doesn't provide data
func GetTotalTimeQuantile(statType StatisticsType, q float64) float64 {
	// Users should query /metrics endpoint instead
	return 0
}

// RecordStartTime records query start time
func RecordStartTime(statType StatisticsType, t time.Time, clientH StatHolder) {
	if clientH != nil {
		clientH.RecordStartTime(statType, t)
	}
}

// RecordFinishedTransaction records completed transaction to Prometheus (lock-free!)
func RecordFinishedTransaction(t time.Time, clientH StatHolder) {
	if clientH == nil {
		return
	}

	clientST := clientH.GetTimeData()
	if clientST == nil {
		return // Graceful handling instead of panic
	}

	// Record router time (end-to-end)
	if !clientST.RouterStart.IsZero() {
		duration := t.Sub(clientST.RouterStart)
		routerDuration.Observe(duration.Seconds()) // NO LOCK!
		clientST.RouterStart = time.Time{}
	}

	// Record shard time (backend execution)
	if !clientST.ShardStart.IsZero() {
		duration := t.Sub(clientST.ShardStart)
		shardDuration.Observe(duration.Seconds()) // NO LOCK!
		clientST.ShardStart = time.Time{}
	}

	// Increment query counter
	queryTotal.Inc()
}

// ConnectionOpened increments active connections
func ConnectionOpened() {
	activeConnections.Inc()
}

// ConnectionClosed decrements active connections
func ConnectionClosed() {
	activeConnections.Dec()
}
