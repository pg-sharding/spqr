package statistics

import (
	"time"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

const (
	MoveStatsTotalTime    = "total"
	MoveStatsRouterTime   = "router"
	MoveStatsShardTime    = "shard"
	MoveStatsQDBPrefix    = "qdb"
	MoveStatsQDBTotalTime = "qdb.Total"
)

type statisticsInt struct {
	CurrentExecTimes     map[string]time.Duration
	TotalTimes           map[string]*MoveStatisticsElem
	CurrentMoveStartTime time.Time
	TotalMoves           int
	MoveInProgress       bool
}

var moveStatistics = statisticsInt{
	CurrentExecTimes: make(map[string]time.Duration),
	TotalTimes:       make(map[string]*MoveStatisticsElem),
}

type MoveStatisticsElem struct {
	TotalDuration time.Duration
	SampleCount   int
}

func RecordMoveStart(t time.Time) error {
	spqrlog.Zero.Debug().Msg("move stats: record move start")
	moveStatistics.MoveInProgress = true
	moveStatistics.CurrentExecTimes = make(map[string]time.Duration)
	moveStatistics.CurrentMoveStartTime = t
	return nil
}

func RecordMoveFinish(t time.Time) error {
	spqrlog.Zero.Debug().Msg("move stats: record move finish")
	if !moveStatistics.MoveInProgress {
		return spqrerror.New(spqrerror.SPQR_UNEXPECTED, "unable to record move finish: there's no move in progress")
	}
	moveStatistics.MoveInProgress = false
	for stat, duration := range moveStatistics.CurrentExecTimes {
		if _, ok := moveStatistics.TotalTimes[stat]; !ok {
			moveStatistics.TotalTimes[stat] = &MoveStatisticsElem{}
		}
		moveStatistics.TotalTimes[stat].SampleCount++
		moveStatistics.TotalTimes[stat].TotalDuration += duration
	}
	if _, ok := moveStatistics.TotalTimes[MoveStatsTotalTime]; !ok {
		moveStatistics.TotalTimes[MoveStatsTotalTime] = &MoveStatisticsElem{}
	}
	moveStatistics.TotalTimes[MoveStatsTotalTime].SampleCount++
	moveStatistics.TotalTimes[MoveStatsTotalTime].TotalDuration += t.Sub(moveStatistics.CurrentMoveStartTime)
	moveStatistics.TotalMoves++
	moveStatistics.CurrentExecTimes = make(map[string]time.Duration)
	return nil
}

func RecordQDBOperation(stat string, duration time.Duration) {
	if moveStatistics.MoveInProgress {
		moveStatistics.CurrentExecTimes[MoveStatsQDBPrefix+"."+stat] += duration
		moveStatistics.CurrentExecTimes[MoveStatsQDBTotalTime] += duration
	}
}

func RecordRouterOperation(duration time.Duration) {
	if moveStatistics.MoveInProgress {
		moveStatistics.CurrentExecTimes[MoveStatsRouterTime] += duration
	}
}

func RecordShardOperation(duration time.Duration) {
	if moveStatistics.MoveInProgress {
		moveStatistics.CurrentExecTimes[MoveStatsShardTime] += duration
	}
}

func GetMoveStats() map[string]time.Duration {
	res := make(map[string]time.Duration)
	for k, v := range moveStatistics.TotalTimes {
		res[k] = v.TotalDuration / time.Duration(v.SampleCount)
	}
	return res
}
