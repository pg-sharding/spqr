package statistics

import (
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

type statisticsInt struct {
	QDBTime              time.Duration
	RouterTime           time.Duration
	ShardTime            time.Duration
	QDBTimeTotal         time.Duration
	RouterTimeTotal      time.Duration
	ShardTimeTotal       time.Duration
	MoveTimeTotal        time.Duration
	CurrentMoveStartTime time.Time
	TotalMoves           int
	MoveInProgress       bool
	lock                 sync.RWMutex
}

var moveStatistics = statisticsInt{}

type MoveStatistics struct {
	RouterTime time.Duration
	ShardTime  time.Duration
	QDBTime    time.Duration
}

func RecordMoveStart(t time.Time) error {
	moveStatistics.MoveInProgress = true
	moveStatistics.QDBTime = 0
	moveStatistics.ShardTime = 0
	moveStatistics.RouterTime = 0
	moveStatistics.CurrentMoveStartTime = t
	return nil
}

func RecordMoveFinish(t time.Time) error {
	if !moveStatistics.MoveInProgress {
		return spqrerror.New(spqrerror.SPQR_UNEXPECTED, "unable to record move finish: there's no move in progress")
	}
	moveStatistics.MoveInProgress = false
	moveStatistics.QDBTimeTotal += moveStatistics.QDBTime
	moveStatistics.RouterTimeTotal += moveStatistics.RouterTime
	moveStatistics.ShardTimeTotal += moveStatistics.ShardTime
	moveStatistics.TotalMoves++
	moveStatistics.QDBTime = 0
	moveStatistics.ShardTime = 0
	moveStatistics.RouterTime = 0
	return nil
}

func RecordQDBOperation(duration time.Duration) {
	if moveStatistics.MoveInProgress {
		moveStatistics.QDBTime += duration
	}
}

func RecordRouterOperation(duration time.Duration) {
	if moveStatistics.MoveInProgress {
		moveStatistics.RouterTime += duration
	}
}

func RecordShardOperation(duration time.Duration) {
	if moveStatistics.MoveInProgress {
		moveStatistics.ShardTime += duration
	}
}

func GetMoveStats() *MoveStatistics {
	return &MoveStatistics{
		ShardTime:  moveStatistics.ShardTimeTotal / time.Duration(moveStatistics.TotalMoves),
		QDBTime:    moveStatistics.QDBTimeTotal / time.Duration(moveStatistics.TotalMoves),
		RouterTime: moveStatistics.RouterTimeTotal / time.Duration(moveStatistics.TotalMoves),
	}
}
