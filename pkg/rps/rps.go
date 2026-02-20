package rps

import (
	"sync"
	"sync/atomic"
	"time"
)

// Clock interface for time injection (enables deterministic testing)
type Clock interface {
	Now() time.Time
}

// realClock uses the actual system time
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// RPSStats tracks requests using a sliding window.
// It's a bit heavier than a simple counter, but gives much smoother "instant" numbers.
// Thread-safe, naturally.
type RPSStats struct {
	mu         sync.RWMutex
	clock      Clock
	windowSize time.Duration
	buckets    []int64 // request counts per bucket
	bucketDur  time.Duration
	numBuckets int
	lastBucket int
	lastTime   time.Time
	total      int64 // requests in current sliding window (NOT the same as totalRequests —
	//                        this gets decremented as old buckets rotate out)
	peakRPS       float64   // highest RPS seen
	totalRequests int64     // lifetime request count (atomic, monotonically increasing)
	startTime     time.Time // when tracking started

	enabled int32 // atomic boolean
}

// Global RPS tracker
var globalRPS *RPSStats
var rpsOnce sync.Once

// NewRPSStats creates a new RPS tracker with a 1-second sliding window
// divided into 10 buckets (100ms each) for smooth measurements.
func NewRPSStats() *RPSStats {
	return NewRPSStatsWithClock(realClock{})
}

// NewRPSStatsWithClock creates an RPS tracker with a custom clock for testing.
func NewRPSStatsWithClock(clock Clock) *RPSStats {
	numBuckets := 10
	windowSize := time.Second
	now := clock.Now()
	// Enabled by default unless explicitly disabled via config?
	// The config default is false (bool zero value), so aggregation starts disabled
	// if config is passed as zero-value bool.

	return &RPSStats{
		clock:         clock,
		windowSize:    windowSize,
		buckets:       make([]int64, numBuckets),
		bucketDur:     windowSize / time.Duration(numBuckets),
		numBuckets:    numBuckets,
		lastBucket:    0,
		lastTime:      now,
		total:         0,
		peakRPS:       0,
		totalRequests: 0,
		startTime:     now,
		enabled:       0, // Disabled by default, enabled via SetEnableRPSAggregation
	}
}

// OnRequest records a new request at the current time.
func (r *RPSStats) OnRequest() {
	r.OnRequestAt(r.clock.Now())
}

// OnRequestAt records a new request at a specific time (for testing).
func (r *RPSStats) OnRequestAt(now time.Time) {
	// Always increment total requests atomically (lock-free)
	atomic.AddInt64(&r.totalRequests, 1)

	// Check if aggregation is enabled
	if atomic.LoadInt32(&r.enabled) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.advanceWindow(now)

	r.buckets[r.lastBucket]++
	r.total++
	// totalRequests is handled atomically above

	// Update peak RPS
	currentRPS := float64(r.total) / r.windowSize.Seconds()
	if currentRPS > r.peakRPS {
		r.peakRPS = currentRPS
	}
}

// advanceWindow moves the sliding window forward and clears old buckets.
// Must be called with the lock held.
func (r *RPSStats) advanceWindow(now time.Time) {
	elapsed := now.Sub(r.lastTime)
	if elapsed < r.bucketDur {
		return // Still in the same bucket
	}

	// Calculate how many buckets to advance
	bucketsToAdvance := int(elapsed / r.bucketDur)
	if bucketsToAdvance >= r.numBuckets {
		// Clear all buckets - window has completely rotated
		for i := range r.buckets {
			r.buckets[i] = 0
		}
		r.total = 0
		r.lastBucket = 0
	} else {
		// We've moved forward, so these intermediate buckets are ancient history. Clear 'em.
		for range bucketsToAdvance {
			nextBucket := (r.lastBucket + 1) % r.numBuckets
			r.total -= r.buckets[nextBucket]
			if r.total < 0 {
				r.total = 0
			}
			r.buckets[nextBucket] = 0
			r.lastBucket = nextBucket
		}
	}

	r.lastTime = now
}

// Snapshot returns the current requests per second rate.
func (r *RPSStats) Snapshot() float64 {
	return r.SnapshotAt(r.clock.Now())
}

// SnapshotAt returns the RPS at a specific time (for testing).
func (r *RPSStats) SnapshotAt(now time.Time) float64 {
	if atomic.LoadInt32(&r.enabled) == 0 {
		return 0.0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.advanceWindow(now)
	return float64(r.total) / r.windowSize.Seconds()
}

// RPSFullSnapshot contains all RPS metrics
type RPSFullSnapshot struct {
	CurrentRPS    float64
	AvgRPS        float64
	PeakRPS       float64
	TotalRequests int64
}

// FullSnapshot returns current, average, and peak RPS.
func (r *RPSStats) FullSnapshot() RPSFullSnapshot {
	return r.FullSnapshotAt(r.clock.Now())
}

// FullSnapshotAt returns metrics at a specific time (for testing).
func (r *RPSStats) FullSnapshotAt(now time.Time) RPSFullSnapshot {
	// Always get total requests
	totalReqs := atomic.LoadInt64(&r.totalRequests)

	if atomic.LoadInt32(&r.enabled) == 0 {
		return RPSFullSnapshot{
			CurrentRPS:    0,
			AvgRPS:        0,
			PeakRPS:       0,
			TotalRequests: totalReqs,
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.advanceWindow(now)

	// Current RPS from sliding window
	currentRPS := float64(r.total) / r.windowSize.Seconds()

	// Average RPS over lifetime
	elapsed := now.Sub(r.startTime).Seconds()
	avgRPS := 0.0
	if elapsed > 0 {
		avgRPS = float64(totalReqs) / elapsed
	}

	return RPSFullSnapshot{
		CurrentRPS:    currentRPS,
		AvgRPS:        avgRPS,
		PeakRPS:       r.peakRPS,
		TotalRequests: totalReqs,
	}
}

// InitRPSStats initializes the global RPS tracker.
func InitRPSStats() {
	rpsOnce.Do(func() {
		globalRPS = NewRPSStats()
	})
}

// InitRPSStatsWithClock initializes the global RPS tracker with a custom clock (for testing).
// Resets the tracker if already initialized.
func InitRPSStatsWithClock(clock Clock) {
	rpsOnce = sync.Once{}
	rpsOnce.Do(func() {
		globalRPS = NewRPSStatsWithClock(clock)
	})
}

// OnRequest records a request to the global RPS tracker.
// Auto-initializes on first call.
func OnRequest() {
	rpsOnce.Do(func() {
		globalRPS = NewRPSStats()
	})
	globalRPS.OnRequest()
}

// SetEnableRPSAggregation enables or disables the expensive sliding window aggregation.
// Total requests are always counted regardless of this setting.
func SetEnableRPSAggregation(enable bool) {
	InitRPSStats() // Ensure initialized
	val := int32(0)
	if enable {
		val = 1
	}
	atomic.StoreInt32(&globalRPS.enabled, val)
}

// GetRPSSnapshot returns the current RPS from the global tracker.
func GetRPSSnapshot() float64 {
	if globalRPS != nil {
		return globalRPS.Snapshot()
	}
	return 0.0
}

// GetRPSFullSnapshot returns current, avg, and peak RPS from global tracker.
func GetRPSFullSnapshot() RPSFullSnapshot {
	if globalRPS != nil {
		return globalRPS.FullSnapshot()
	}
	return RPSFullSnapshot{}
}

// GetRPSStats returns the global RPS stats instance.
func GetRPSStats() *RPSStats {
	return globalRPS
}

// ResetRPSStats resets the global RPS tracker (for testing).
func ResetRPSStats() {
	rpsOnce = sync.Once{}
	globalRPS = nil
}

// GetTotal returns the current total count (for testing).
func (r *RPSStats) GetTotal() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.total
}

// GetTotalRequests returns the lifetime request count (thread-safe).
func (r *RPSStats) GetTotalRequests() int64 {
	return atomic.LoadInt64(&r.totalRequests)
}

// RouterRPSCmd returns the console command name for RPS stats.
func RouterRPSCmd() string {
	return "router_rps"
}
