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
}

var queryStatistics = statistics{
	RouterTime: make(map[string]*tdigest.TDigest),
	ShardTime:  make(map[string]*tdigest.TDigest),
	TimeData:   make(map[string]*startTimes),
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
	queryStatistics.RouterTime[client].Add(float64(t.Sub(queryStatistics.TimeData[client].RouterStart).Microseconds()))
	queryStatistics.ShardTime[client].Add(float64(t.Sub(queryStatistics.TimeData[client].ShardStart).Microseconds()))
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
