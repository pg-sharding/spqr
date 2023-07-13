package statistics

import (
	"time"

	"github.com/caio/go-tdigest"
)

type StartTimeType string

const (
	StartRouter = StartTimeType("router")
	StartShard  = StartTimeType("shard")
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

func RecordStartTime(tip StartTimeType, t time.Time, user string) {
	if queryStatistics.TimeData[user] == nil {
		queryStatistics.TimeData[user] = &startTimes{}
	}
	switch tip {
	case StartRouter:
		queryStatistics.TimeData[user].RouterStart = t
	case StartShard:
		queryStatistics.TimeData[user].ShardStart = t
	}
}

func RecordFinishedTransaction(t time.Time, user string) {
	if queryStatistics.RouterTime[user] == nil {
		queryStatistics.RouterTime[user], _ = tdigest.New()
	}
	if queryStatistics.ShardTime[user] == nil {
		queryStatistics.ShardTime[user], _ = tdigest.New()
	}
	queryStatistics.RouterTime[user].Add(float64(t.Sub(queryStatistics.TimeData[user].RouterStart).Nanoseconds()))
	queryStatistics.ShardTime[user].Add(float64(t.Sub(queryStatistics.TimeData[user].ShardStart).Nanoseconds()))
}
