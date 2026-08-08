package statistics

import (
	"sync"
	"sync/atomic"
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

var LockStats = &keyRangeLockStats{
	mu:               sync.RWMutex{},
	keyRangeLockTime: map[string]time.Time{},
}

type keyRangeLockStats struct {
	mu               sync.RWMutex
	keyRangeLockTime map[string]time.Time

	opsCount     int
	opsTotalTime time.Duration
}

func (s *keyRangeLockStats) RecordLockKeyRange(keyRangeID string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.keyRangeLockTime[keyRangeID] = t
}

func (s *keyRangeLockStats) RecordUnlockKeyRange(keyRangeID string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lockTime, ok := s.keyRangeLockTime[keyRangeID]
	if !ok {
		return
	}
	s.opsCount++
	s.opsTotalTime += t.Sub(lockTime)
}

func (s *keyRangeLockStats) GetMeanLockTime() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.opsCount == 0 {
		return 0
	}
	return s.opsTotalTime / time.Duration(s.opsCount)
}

type statisticsInt struct {
	// currentExecTimes is swapped atomically on each move start/finish.
	currentExecTimes     atomic.Pointer[sync.Map]
	TotalTimes           map[string]*MoveStatisticsElem
	CurrentMoveStartTime time.Time
	TotalMoves           int
	moveInProgress       atomic.Bool
	totalTimesMu         sync.Mutex
}

var moveStatistics = func() statisticsInt {
	var s statisticsInt
	s.currentExecTimes.Store(&sync.Map{})
	s.TotalTimes = make(map[string]*MoveStatisticsElem)
	return s
}()

type MoveStatisticsElem struct {
	TotalDuration time.Duration
	SampleCount   int
}

func RecordMoveStart(t time.Time) error {
	spqrlog.Zero.Debug().Msg("move stats: record move start")
	moveStatistics.totalTimesMu.Lock()
	defer moveStatistics.totalTimesMu.Unlock()
	moveStatistics.currentExecTimes.Store(&sync.Map{})
	moveStatistics.CurrentMoveStartTime = t
	moveStatistics.moveInProgress.Store(true)
	return nil
}

func RecordMoveFinish(t time.Time) error {
	spqrlog.Zero.Debug().Msg("move stats: record move finish")
	if !moveStatistics.moveInProgress.Load() {
		return spqrerror.New(spqrerror.SPQR_UNEXPECTED, "unable to record move finish: there's no move in progress")
	}
	moveStatistics.totalTimesMu.Lock()
	defer moveStatistics.totalTimesMu.Unlock()
	moveStatistics.moveInProgress.Store(false)
	finishedMap := moveStatistics.currentExecTimes.Swap(&sync.Map{})
	finishedMap.Range(func(key, value any) bool {
		stat, ok := key.(string)
		if !ok {
			return true
		}
		duration, ok := value.(time.Duration)
		if !ok {
			return true
		}
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
	return nil
}

func RecordQDBOperation(stat string, duration time.Duration) {
	if moveStatistics.moveInProgress.Load() {
		m := moveStatistics.currentExecTimes.Load()
		statName := MoveStatsQDBPrefix + "." + stat
		curValue, ok := m.Load(statName)
		if ok {
			m.Store(statName, curValue.(time.Duration)+duration)
		} else {
			m.Store(statName, duration)
		}
		curValue, ok = m.Load(MoveStatsQDBTotalTime)
		if ok {
			m.Store(MoveStatsQDBTotalTime, curValue.(time.Duration)+duration)
		} else {
			m.Store(MoveStatsQDBTotalTime, duration)
		}
	}
}

func RecordRouterOperation(duration time.Duration) {
	if moveStatistics.moveInProgress.Load() {
		m := moveStatistics.currentExecTimes.Load()
		curValue, ok := m.Load(MoveStatsRouterTime)
		if ok {
			m.Store(MoveStatsRouterTime, curValue.(time.Duration)+duration)
		} else {
			m.Store(MoveStatsRouterTime, duration)
		}
	}
}

func RecordShardOperation(stat string, duration time.Duration) {
	if moveStatistics.moveInProgress.Load() {
		m := moveStatistics.currentExecTimes.Load()
		statName := MoveStatsShardPrefix + "." + stat
		curValue, ok := m.Load(statName)
		if ok {
			m.Store(statName, curValue.(time.Duration)+duration)
		} else {
			m.Store(statName, duration)
		}
		curValue, ok = m.Load(MoveStatsShardTotalTime)
		if ok {
			m.Store(MoveStatsShardTotalTime, curValue.(time.Duration)+duration)
		} else {
			m.Store(MoveStatsShardTotalTime, duration)
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
