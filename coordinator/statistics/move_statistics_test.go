package statistics

import (
	"sync"
	"testing"
	"time"
)

func resetMoveStatistics() {
	moveStatistics.totalTimesMu.Lock()
	defer moveStatistics.totalTimesMu.Unlock()
	moveStatistics.moveInProgress.Store(false)
	moveStatistics.currentExecTimes.Store(&sync.Map{})
	moveStatistics.CurrentMoveStartTime = time.Time{}
	moveStatistics.TotalTimes = make(map[string]*MoveStatisticsElem)
	moveStatistics.TotalMoves = 0
}

func resetLockStats() {
	LockStats.mu.Lock()
	defer LockStats.mu.Unlock()
	LockStats.keyRangeLockTime = map[string]time.Time{}
	LockStats.opsCount = 0
	LockStats.opsTotalTime = 0
}

func TestRace_MoveInProgress_Concurrent(t *testing.T) {
	resetMoveStatistics()
	resetLockStats()

	const cycles = 30
	const readers = 10

	var wg sync.WaitGroup
	wg.Add(3 /* aux runners */ + readers)

	go func() {
		defer wg.Done()
		for range cycles {
			_ = RecordMoveStart(time.Now())
			time.Sleep(time.Microsecond)
			_ = RecordMoveFinish(time.Now())
		}
	}()
	
	go func() {
		defer wg.Done()
		for range cycles {
			_ = RecordMoveStart(time.Now())
			_ = RecordMoveFinish(time.Now())
		}
	}()

	go func() {
		defer wg.Done()
		for range cycles * 5 {
			_ = GetMoveStats()
		}
	}()

	for range readers {
		go func() {
			defer wg.Done()
			for range cycles * 10 {
				RecordQDBOperation("read", time.Microsecond)
				RecordRouterOperation(time.Microsecond)
				RecordShardOperation("query", time.Microsecond)
			}
		}()
	}

	wg.Wait()
}

func TestRace_AllOperations_Concurrent(t *testing.T) {
	resetMoveStatistics()
	resetLockStats()

	if err := RecordMoveStart(time.Now()); err != nil {
		t.Fatal(err)
	}

	const goroutines = 20
	const iters = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 6)

	for i := range goroutines {
		id := "kr" + string(rune('A'+i%26))

		go func() {
			defer wg.Done()
			for range iters {
				RecordQDBOperation("write", time.Duration(i)*time.Microsecond)
			}
		}()
		go func() {
			defer wg.Done()
			for range iters {
				RecordRouterOperation(time.Duration(i) * time.Microsecond)
			}
		}()
		go func() {
			defer wg.Done()
			for range iters {
				RecordShardOperation("copy", time.Duration(i)*time.Microsecond)
			}
		}()
		go func() {
			defer wg.Done()
			for range iters {
				LockStats.RecordLockKeyRange(id, time.Now())
			}
		}()
		go func() {
			defer wg.Done()
			for range iters {
				LockStats.RecordUnlockKeyRange(id, time.Now())
			}
		}()
		go func() {
			defer wg.Done()
			for range iters {
				_ = LockStats.GetMeanLockTime()
			}
		}()
	}

	wg.Wait()

	if err := RecordMoveFinish(time.Now()); err != nil {
		t.Fatal(err)
	}
}
