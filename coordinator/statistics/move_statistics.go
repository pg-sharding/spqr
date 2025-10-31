package statistics

import (
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

const (
	MoveStatsTotalTime      = "total"
	MoveStatsRouterTime     = "router"
	MoveStatsShardPrefix    = "shard"
	MoveStatsShardTotalTime = "shard.Total"
	MoveStatsQDBPrefix      = "qdb"
	MoveStatsQDBTotalTime   = "qdb.Total"
)

type statisticsInt struct {
	CurrentExecTimes     sync.Map
	TotalTimes           map[string]*MoveStatisticsElem
	CurrentMoveStartTime time.Time
	TotalMoves           int
	MoveInProgress       bool
	totalTimesMu         sync.Mutex
}

var moveStatistics = statisticsInt{
	CurrentExecTimes: sync.Map{},
	TotalTimes:       make(map[string]*MoveStatisticsElem),
}

type MoveStatisticsElem struct {
	TotalDuration time.Duration
	SampleCount   int
}

func RecordMoveStart(t time.Time) error {
	spqrlog.Zero.Debug().Msg("move stats: record move start")
	moveStatistics.totalTimesMu.Lock()
	defer moveStatistics.totalTimesMu.Unlock()
	moveStatistics.MoveInProgress = true
	moveStatistics.CurrentExecTimes = sync.Map{}
	moveStatistics.CurrentMoveStartTime = t
	return nil
}

func RecordMoveFinish(t time.Time) error {
	spqrlog.Zero.Debug().Msg("move stats: record move finish")
	if !moveStatistics.MoveInProgress {
		return spqrerror.New(spqrerror.SPQR_UNEXPECTED, "unable to record move finish: there's no move in progress")
	}
	moveStatistics.totalTimesMu.Lock()
	defer moveStatistics.totalTimesMu.Unlock()
	moveStatistics.MoveInProgress = false
	moveStatistics.CurrentExecTimes.Range(func(key, value any) bool {
		stat := key.(string)
		duration := value.(time.Duration)
		if _, ok := moveStatistics.TotalTimes[stat]; !ok {
			moveStatistics.TotalTimes[stat] = &MoveStatisticsElem{}
		}
		moveStatistics.TotalTimes[stat].SampleCount++
		moveStatistics.TotalTimes[stat].TotalDuration += duration
		return true
	})
	if _, ok := moveStatistics.TotalTimes[MoveStatsTotalTime]; !ok {
		moveStatistics.TotalTimes[MoveStatsTotalTime] = &MoveStatisticsElem{}
	}
	moveStatistics.TotalTimes[MoveStatsTotalTime].SampleCount++
	moveStatistics.TotalTimes[MoveStatsTotalTime].TotalDuration += t.Sub(moveStatistics.CurrentMoveStartTime)
	moveStatistics.TotalMoves++
	moveStatistics.CurrentExecTimes = sync.Map{}
	return nil
}

func RecordQDBOperation(stat string, duration time.Duration) {
	if moveStatistics.MoveInProgress {
		statName := MoveStatsQDBPrefix + "." + stat
		curValue, ok := moveStatistics.CurrentExecTimes.Load(statName)
		if ok {
			moveStatistics.CurrentExecTimes.Store(statName, curValue.(time.Duration)+duration)
		} else {
			moveStatistics.CurrentExecTimes.Store(statName, duration)
		}
		curValue, ok = moveStatistics.CurrentExecTimes.Load(MoveStatsQDBTotalTime)
		if ok {
			moveStatistics.CurrentExecTimes.Store(MoveStatsQDBTotalTime, curValue.(time.Duration)+duration)
		} else {
			moveStatistics.CurrentExecTimes.Store(MoveStatsQDBTotalTime, duration)
		}
	}
}

func RecordRouterOperation(duration time.Duration) {
	if moveStatistics.MoveInProgress {
		curValue, ok := moveStatistics.CurrentExecTimes.Load(MoveStatsRouterTime)
		if ok {
			moveStatistics.CurrentExecTimes.Store(MoveStatsRouterTime, curValue.(time.Duration)+duration)
		} else {
			moveStatistics.CurrentExecTimes.Store(MoveStatsRouterTime, duration)
		}
	}
}

func RecordShardOperation(stat string, duration time.Duration) {
	if moveStatistics.MoveInProgress {
		statName := MoveStatsShardPrefix + "." + stat
		curValue, ok := moveStatistics.CurrentExecTimes.Load(statName)
		if ok {
			moveStatistics.CurrentExecTimes.Store(statName, curValue.(time.Duration)+duration)
		} else {
			moveStatistics.CurrentExecTimes.Store(statName, duration)
		}
		curValue, ok = moveStatistics.CurrentExecTimes.Load(MoveStatsShardTotalTime)
		if ok {
			moveStatistics.CurrentExecTimes.Store(MoveStatsShardTotalTime, curValue.(time.Duration)+duration)
		} else {
			moveStatistics.CurrentExecTimes.Store(MoveStatsShardTotalTime, duration)
		}
	}
}

func GetMoveStats() map[string]time.Duration {
	moveStatistics.totalTimesMu.Lock()
	defer moveStatistics.totalTimesMu.Unlock()
	res := make(map[string]time.Duration)
	for k, v := range moveStatistics.TotalTimes {
		res[k] = v.TotalDuration / time.Duration(v.SampleCount)
	}
	return res
}
