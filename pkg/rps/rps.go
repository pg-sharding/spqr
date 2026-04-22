package rps

import (
	"sync"
	"sync/atomic"
)

// RPSStats tracks requests using a sliding window.
// It's a bit heavier than a simple counter, but gives much smoother "instant" numbers.
// Thread-safe, naturally.
type RPSStats struct {
	mu sync.RWMutex

	totalRequests int64 // lifetime request count (atomic, monotonically increasing)
}

// Global RPS tracker
var globalRPS *RPSStats
var rpsOnce sync.Once

// NewRPSStats creates a new RPS tracker with a 1-second sliding window
// divided into 10 buckets (100ms each) for smooth measurements.
func NewRPSStats() *RPSStats {
	return &RPSStats{
		totalRequests: 0,
	}
}

// OnRequest records a new request at the current time.
func (r *RPSStats) OnRequest() {
	atomic.AddInt64(&r.totalRequests, 1)
}

// InitRPSStats initializes the global RPS tracker.
func InitRPSStats() {
	rpsOnce.Do(func() {
		globalRPS = NewRPSStats()
	})
}

// InitRPSStatsWithClock initializes the global RPS tracker with a custom clock (for testing).
// Resets the tracker if already initialized.
func InitRPSStatsWithClock() {
	rpsOnce = sync.Once{}
	rpsOnce.Do(func() {
		globalRPS = NewRPSStats()
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

// GetRPSStats returns the global RPS stats instance.
func GetRPSStats() *RPSStats {
	return globalRPS
}

// ResetRPSStats resets the global RPS tracker (for testing).
func ResetRPSStats() {
	rpsOnce = sync.Once{}
	globalRPS = nil
}

// GetTotalRequests returns the lifetime request count (thread-safe).
func (r *RPSStats) GetTotalRequests() int64 {
	return atomic.LoadInt64(&r.totalRequests)
}
