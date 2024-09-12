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
	RouterTime        map[uint]*tdigest.TDigest
	ShardTime         map[uint]*tdigest.TDigest
	RouterTimeTotal   *tdigest.TDigest
	ShardTimeTotal    *tdigest.TDigest
	TimeData          map[uint]*startTimes
	Quantiles         []float64
	NeedToCollectData bool
	lock              sync.RWMutex
}

var queryStatistics = statistics{
	RouterTime:      make(map[uint]*tdigest.TDigest),
	ShardTime:       make(map[uint]*tdigest.TDigest),
	RouterTimeTotal: nil,
	ShardTimeTotal:  nil,
	TimeData:        make(map[uint]*startTimes),
	lock:            sync.RWMutex{},
}

func InitStatistics(q []float64) {
	queryStatistics.Quantiles = q

	if len(queryStatistics.Quantiles) > 0 { // also not nil
		queryStatistics.NeedToCollectData = false
	} else {
		queryStatistics.NeedToCollectData = true
	}

	queryStatistics.RouterTimeTotal, _ = tdigest.New()
	queryStatistics.ShardTimeTotal, _ = tdigest.New()
}

func GetQuantiles() *[]float64 {
	return &queryStatistics.Quantiles
}

func GetTimeQuantile(statType StatisticsType, q float64, client uint) float64 {
	queryStatistics.lock.Lock()
	defer queryStatistics.lock.Unlock()

	var stat *tdigest.TDigest

	switch statType {
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

func GetTotalTimeQuantile(statType StatisticsType, q float64) float64 {
	queryStatistics.lock.Lock()
	defer queryStatistics.lock.Unlock()

	switch statType {
	case Router:
		if queryStatistics.RouterTimeTotal == nil || queryStatistics.RouterTimeTotal.Count() == 0 {
			return 0
		}
		return queryStatistics.RouterTimeTotal.Quantile(q)
	case Shard:
		if queryStatistics.ShardTimeTotal == nil || queryStatistics.ShardTimeTotal.Count() == 0 {
			return 0
		}
		return queryStatistics.ShardTimeTotal.Quantile(q)
	default:
		return 0
	}
}

func RecordStartTime(statType StatisticsType, t time.Time, client uint) {
	if queryStatistics.NeedToCollectData {
		return
	}

	queryStatistics.lock.Lock()
	defer queryStatistics.lock.Unlock()

	if queryStatistics.TimeData[client] == nil {
		queryStatistics.TimeData[client] = &startTimes{}
	}
	switch statType {
	case Router:
		queryStatistics.TimeData[client].RouterStart = t
	case Shard:
		queryStatistics.TimeData[client].ShardStart = t
	}
}

func RecordFinishedTransaction(t time.Time, client uint) {
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
	if queryStatistics.RouterTimeTotal == nil {
		queryStatistics.RouterTimeTotal, _ = tdigest.New()
	}
	if queryStatistics.ShardTimeTotal == nil {
		queryStatistics.ShardTimeTotal, _ = tdigest.New()
	}
	routerTime := float64(t.Sub(queryStatistics.TimeData[client].RouterStart).Microseconds()) / 1000
	shardTime := float64(t.Sub(queryStatistics.TimeData[client].ShardStart).Microseconds()) / 1000
	err := queryStatistics.RouterTime[client].Add(routerTime)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Failed to record transaction duration")
	}
	err = queryStatistics.ShardTime[client].Add(shardTime)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Failed to record transaction duration")
	}
	err = queryStatistics.RouterTimeTotal.Add(routerTime)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Failed to record transaction duration")
	}
	err = queryStatistics.ShardTimeTotal.Add(shardTime)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Failed to record transaction duration")
	}
}
