package provider

const (
	cpuMetric = iota
	spaceMetric
	metricsCount // insert new metric types above
)

type ShardMetrics struct {
	ShardId       string
	MetricsTotal  []float64
	MetricsKR     map[string][]float64 // mean value for object by key range
	KeyCountKR    map[string]int64
	KeyCountRelKR map[string]map[string]int64
	Master        string
	TargetReplica string
}

type HostMetrics []float64

func NewHostMetrics() HostMetrics {
	return make([]float64, metricsCount)
}

func NewShardMetrics() *ShardMetrics {
	return &ShardMetrics{
		MetricsTotal:  make([]float64, 2*metricsCount),
		MetricsKR:     make(map[string][]float64),
		KeyCountKR:    make(map[string]int64),
		KeyCountRelKR: make(map[string]map[string]int64),
	}
}

func (m *ShardMetrics) SetMasterMetrics(metrics HostMetrics) {
	for i := range metricsCount {
		m.MetricsTotal[i] = metrics[i]
	}
}

func (m *ShardMetrics) SetReplicaMetrics(metrics HostMetrics) {
	for i := range metricsCount {
		m.MetricsTotal[i+metricsCount] = metrics[i]
	}
}

func (m HostMetrics) MaxRelative(threshold []float64) (val float64) {
	val, _ = MaxRelative(m, threshold)
	return val
}

func MaxRelative(metrics, threshold []float64) (val float64, kind int) {
	if len(metrics) != len(threshold) {
		panic("incorrect size of threshold")
	}

	val = -1
	for k, metric := range metrics {
		relative := metric / threshold[k]
		if relative > val {
			val = relative
			kind = k
		}
	}
	return
}
