package statistics

import (
	"time"

	"github.com/caio/go-tdigest"
)

type StatisticsType string

const (
	Router = StatisticsType("router")
	Shard  = StatisticsType("shard")
)

type startTimes struct {
	RouterStart time.Time
	ShardStart  time.Time
}

type statistics struct {
	RouterTime map[string]*tdigest.TDigest
	ShardTime  map[string]*tdigest.TDigest
	TimeData   map[string]*startTimes
	Quantiles  []float64
}

var queryStatistics = statistics{
	RouterTime: make(map[string]*tdigest.TDigest),
	ShardTime:  make(map[string]*tdigest.TDigest),
	TimeData:   make(map[string]*startTimes),
}

func SetQuantiles(q []float64) {
	queryStatistics.Quantiles = q
}

func GetQuantiles() *[]float64 {
	return &queryStatistics.Quantiles
}

func RecordStartTime(tip StatisticsType, t time.Time, client string) {
	if queryStatistics.TimeData[client] == nil {
		queryStatistics.TimeData[client] = &startTimes{}
	}
	switch tip {
	case Router:
		queryStatistics.TimeData[client].RouterStart = t
	case Shard:
		queryStatistics.TimeData[client].ShardStart = t
	}
}

func RecordFinishedTransaction(t time.Time, client string) {
	if queryStatistics.RouterTime[client] == nil {
		queryStatistics.RouterTime[client], _ = tdigest.New()
	}
	if queryStatistics.ShardTime[client] == nil {
		queryStatistics.ShardTime[client], _ = tdigest.New()
	}
	queryStatistics.RouterTime[client].Add(float64(t.Sub(queryStatistics.TimeData[client].RouterStart).Microseconds()) / 1000)
	queryStatistics.ShardTime[client].Add(float64(t.Sub(queryStatistics.TimeData[client].ShardStart).Microseconds()) / 1000)
}

func GetClientTimeStatistics(tip StatisticsType, client string) *tdigest.TDigest {
	var stat *tdigest.TDigest
	switch tip {
	case Router:
		stat = queryStatistics.RouterTime[client]
	case Shard:
		stat = queryStatistics.ShardTime[client]
	}

	if stat == nil {
		stat, _ = tdigest.New()
	}
	return stat
}
