package statistics

import (
	"sync"
	"time"

	"github.com/caio/go-tdigest"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
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
	RouterTime        map[string]*tdigest.TDigest
	ShardTime         map[string]*tdigest.TDigest
	TimeData          map[string]*startTimes
	Quantiles         []float64
	NeedToCollectData bool
	lock              sync.RWMutex
}

var queryStatistics = statistics{
	RouterTime: make(map[string]*tdigest.TDigest),
	ShardTime:  make(map[string]*tdigest.TDigest),
	TimeData:   make(map[string]*startTimes),
	lock:       sync.RWMutex{},
}

func InitStatistics(q []float64) {
	queryStatistics.Quantiles = q
	if queryStatistics.Quantiles != nil && len(queryStatistics.Quantiles) > 0 {
		queryStatistics.NeedToCollectData = false
	} else {
		queryStatistics.NeedToCollectData = true
	}
}

func GetQuantiles() *[]float64 {
	return &queryStatistics.Quantiles
}

func GetTimeQuantile(tip StatisticsType, q float64, client string) float64 {
	queryStatistics.lock.Lock()
	defer queryStatistics.lock.Unlock()

	var stat *tdigest.TDigest

	switch tip {
	case Router:
		stat = queryStatistics.RouterTime[client]
		if stat == nil {
			return 0
		}
		return stat.Quantile(q)
	case Shard:
		stat = queryStatistics.ShardTime[client]
		if stat == nil {
			return 0
		}
		return stat.Quantile(q)
	default:
		return 0
	}
}

func RecordStartTime(tip StatisticsType, t time.Time, client string) {
	if queryStatistics.NeedToCollectData {
		return
	}

	queryStatistics.lock.Lock()
	defer queryStatistics.lock.Unlock()

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
	if queryStatistics.NeedToCollectData {
		return
	}

	queryStatistics.lock.Lock()
	defer queryStatistics.lock.Unlock()

	if queryStatistics.RouterTime[client] == nil {
		queryStatistics.RouterTime[client], _ = tdigest.New()
	}
	if queryStatistics.ShardTime[client] == nil {
		queryStatistics.ShardTime[client], _ = tdigest.New()
	}
	err := queryStatistics.RouterTime[client].Add(float64(t.Sub(queryStatistics.TimeData[client].RouterStart).Microseconds()) / 1000)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg(err.Error())
	}
	err = queryStatistics.ShardTime[client].Add(float64(t.Sub(queryStatistics.TimeData[client].ShardStart).Microseconds()) / 1000)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg(err.Error())
	}
}
