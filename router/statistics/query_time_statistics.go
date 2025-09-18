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
	StatisticsTypeRouter = StatisticsType("router")
	StatisticsTypeShard  = StatisticsType("shard")
)

type StartTimes struct {
	RouterStart time.Time
	ShardStart  time.Time
}

type Statistics struct {
	RouterTime        map[uint]*tdigest.TDigest
	ShardTime         map[uint]*tdigest.TDigest
	RouterTimeTotal   *tdigest.TDigest
	ShardTimeTotal    *tdigest.TDigest
	TimeData          map[uint]*StartTimes
	Quantiles         []float64
	QuantilesStr      []string
	NeedToCollectData bool
	lock              sync.RWMutex
}

var QueryStatistics = Statistics{
	RouterTime:      make(map[uint]*tdigest.TDigest),
	ShardTime:       make(map[uint]*tdigest.TDigest),
	RouterTimeTotal: nil,
	ShardTimeTotal:  nil,
	TimeData:        make(map[uint]*StartTimes),
	lock:            sync.RWMutex{},
}

func InitStatistics(q []float64) {
	QueryStatistics.Quantiles = q
	initStatsCommon()
}

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
	initStatsCommon()
	return nil
}

func initStatsCommon() {
	if len(QueryStatistics.Quantiles) > 0 { // also not nil
		QueryStatistics.NeedToCollectData = true
	} else {
		QueryStatistics.NeedToCollectData = false
	}

	QueryStatistics.RouterTimeTotal, _ = tdigest.New()
	QueryStatistics.ShardTimeTotal, _ = tdigest.New()
}

func GetQuantiles() *[]float64 {
	return &QueryStatistics.Quantiles
}

func GetQuantilesStr() *[]string {
	return &QueryStatistics.QuantilesStr
}

func GetTimeQuantile(statType StatisticsType, q float64, clienth StatHolder) float64 {
	if !QueryStatistics.NeedToCollectData {
		return 0
	}

	return clienth.GetTimeQuantile(statType, q)
}

func GetTotalTimeQuantile(statType StatisticsType, q float64) float64 {
	QueryStatistics.lock.Lock()
	defer QueryStatistics.lock.Unlock()

	switch statType {
	case StatisticsTypeRouter:
		if QueryStatistics.RouterTimeTotal.Count() == 0 {
			return 0
		}
		return QueryStatistics.RouterTimeTotal.Quantile(q)
	case StatisticsTypeShard:
		if QueryStatistics.ShardTimeTotal.Count() == 0 {
			return 0
		}
		return QueryStatistics.ShardTimeTotal.Quantile(q)
	default:
		return 0
	}
}

func RecordStartTime(statType StatisticsType, t time.Time, clientH StatHolder) {
	if !QueryStatistics.NeedToCollectData {
		return
	}

	clientH.RecordStartTime(statType, t)
}

func RecordFinishedTransaction(t time.Time, clientH StatHolder) {
	if !QueryStatistics.NeedToCollectData {
		return
	}

	QueryStatistics.lock.Lock()
	defer QueryStatistics.lock.Unlock()

	clientST := clientH.GetTimeData()
	if clientST == nil {
		panic("finish of unstarted transaction")
	}

	if !clientST.RouterStart.IsZero() {
		routerTime := float64(t.Sub(clientST.RouterStart).Microseconds()) / 1000
		err := clientH.Add(StatisticsTypeRouter, routerTime)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to record transaction duration")
		}
		err = QueryStatistics.RouterTimeTotal.Add(routerTime)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to record transaction duration")
		}
		clientST.RouterStart = time.Time{}
	}
	if !clientST.ShardStart.IsZero() {
		shardTime := float64(t.Sub(clientST.ShardStart).Microseconds()) / 1000
		err := clientH.Add(StatisticsTypeShard, shardTime)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to record transaction duration")
		}
		err = QueryStatistics.ShardTimeTotal.Add(shardTime)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to record transaction duration")
		}
		clientST.ShardStart = time.Time{}
	}
}
