package statistics

import (
	"fmt"
	"strconv"
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
	QuantilesStr      []string
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
	initStatsCommon()
}

func InitStatisticsStr(q []string) error {
	queryStatistics.QuantilesStr = q
	queryStatistics.Quantiles = make([]float64, len(q))
	for i, qStr := range q {
		var err error
		queryStatistics.Quantiles[i], err = strconv.ParseFloat(qStr, 64)
		if err != nil {
			return fmt.Errorf("could not parse time quantile to float: \"%s\"", qStr)
		}
	}
	initStatsCommon()
	return nil
}

func initStatsCommon() {
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

func GetQuantilesStr() *[]string {
	return &queryStatistics.QuantilesStr
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

	var clientST *startTimes
	var ok bool
	if clientST, ok = queryStatistics.TimeData[client]; !ok {
		panic("finish of unstarted transaction")
	}

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

	if !clientST.RouterStart.IsZero() {
		routerTime := float64(t.Sub(clientST.RouterStart).Microseconds()) / 1000
		err := queryStatistics.RouterTime[client].Add(routerTime)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to record transaction duration")
		}
		err = queryStatistics.RouterTimeTotal.Add(routerTime)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to record transaction duration")
		}
		clientST.RouterStart = time.Time{}
	}
	if !clientST.ShardStart.IsZero() {
		shardTime := float64(t.Sub(clientST.ShardStart).Microseconds()) / 1000
		err := queryStatistics.ShardTime[client].Add(shardTime)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to record transaction duration")
		}
		err = queryStatistics.ShardTimeTotal.Add(shardTime)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to record transaction duration")
		}
		clientST.ShardStart = time.Time{}
	}
}
