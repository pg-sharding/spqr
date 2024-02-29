package provider

const (
	cpuMetric = iota
	spaceMetric
	metricsCount // insert new metric types above
)

type ShardMetrics struct {
	ShardId      string
	MetricsTotal Metrics
	MetricsKR    map[string]Metrics
	KeyCount     map[string]int64
}

type Metrics struct {
	metrics []float64
}

func NewMetrics() *Metrics {
	return &Metrics{
		metrics: make([]float64, 2*metricsCount),
	}
}
