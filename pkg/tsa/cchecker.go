package tsa

import (
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/shard"
)

type CacheEntry struct {
	result    bool
	comment   string
	lastCheck int64
}

type CachedTSAChecker struct {
	mu            sync.Mutex
	recheckPeriod time.Duration
	cache         map[string]CacheEntry
	innerChecker  TSAChecker
}

var _ TSAChecker = (*CachedTSAChecker)(nil)

// NewTSAChecker creates a new instance of TSAChecker.
// It returns a TSAChecker interface that can be used to perform TSA checks.
//
// Returns:
//   - TSAChecker: A new instance of TSAChecker.
func NewTSAChecker() TSAChecker {
	return &CachedTSAChecker{
		mu:            sync.Mutex{},
		recheckPeriod: time.Second,
		cache:         map[string]CacheEntry{},
		innerChecker:  &Checker{},
	}
}

func NewTSACheckerWithDuration(tsaRecheckDuration time.Duration) TSAChecker {
	return &CachedTSAChecker{
		mu:            sync.Mutex{},
		recheckPeriod: tsaRecheckDuration,
		cache:         map[string]CacheEntry{},
		innerChecker:  &Checker{},
	}
}

// CheckTSA checks the TSA for a given shard and returns the result, comment, and error.
// If the TSA check result is already cached and not expired, it returns the cached result.
// Otherwise, it performs the TSA check and updates the cache with the new result.
// The function returns a boolean indicating whether the shard is in a read-write state,
// a string describing the reason for the state, and an error if any occurred during the process.
//
// Parameters:
//   - sh: The shard to check the TSA for.
//
// Returns:
//   - bool: A boolean indicating whether the shard is in a read-write state.
//   - string: A string describing the reason for the state.
//   - error: An error if any occurred during the process.
func (ctsa *CachedTSAChecker) CheckTSA(sh shard.Shard) (CheckResult, error) {
	ctsa.mu.Lock()
	defer ctsa.mu.Unlock()

	n := time.Now().UnixNano()
	if e, ok := ctsa.cache[sh.Instance().Hostname()]; ok && n-e.lastCheck < ctsa.recheckPeriod.Nanoseconds() {
		return CheckResult{
			RW:     e.result,
			Reason: e.comment,
		}, nil
	}

	cr, err := ctsa.innerChecker.CheckTSA(sh)
	if err != nil {
		return cr, err
	}
	ctsa.cache[sh.Instance().Hostname()] = CacheEntry{
		lastCheck: n,
		comment:   cr.Reason,
		result:    cr.RW,
	}
	return cr, nil
}
