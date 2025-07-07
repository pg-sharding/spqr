package tsa

import (
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/shard"
)

type CachedTSAChecker struct {
	mu            sync.Mutex
	recheckPeriod time.Duration
	cache         map[string]TimedCheckResult
	innerChecker  TSAChecker
}

var _ TimedTSAChecker = (*CachedTSAChecker)(nil)

// NewTSAChecker creates a new instance of TSAChecker.
// It returns a TSAChecker interface that can be used to perform TSA checks.
//
// Returns:
//   - TSAChecker: A new instance of TSAChecker.
func NewTSAChecker() TimedTSAChecker {
	return &CachedTSAChecker{
		mu:            sync.Mutex{},
		recheckPeriod: time.Second,
		cache:         map[string]TimedCheckResult{},
		innerChecker:  &NetChecker{},
	}
}

func NewTSACheckerWithDuration(tsaRecheckDuration time.Duration) TimedTSAChecker {
	return &CachedTSAChecker{
		mu:            sync.Mutex{},
		recheckPeriod: tsaRecheckDuration,
		cache:         map[string]TimedCheckResult{},
		innerChecker:  &NetChecker{},
	}
}

// CheckTSA checks the TSA for a given shard and returns the result, comment, and error.
// If the TSA check result is already cached and not expired, it returns the cached result.
// Otherwise, it performs the TSA check and updates the cache with the new result.
//
// Parameters:
//   - sh: The shard to check the TSA for.
//
// Returns:
//   - CheckResult: A struct containing the result of the TSA check.
//   - error: An error if any occurred during the process.
func (ctsa *CachedTSAChecker) CheckTSA(sh shard.ShardHostInstance) (TimedCheckResult, error) {
	ctsa.mu.Lock()
	defer ctsa.mu.Unlock()

	n := time.Now()
	if e, ok := ctsa.cache[sh.Instance().Hostname()]; ok && n.UnixNano()-e.T.UnixNano() < ctsa.recheckPeriod.Nanoseconds() {
		return e, nil
	}

	cr, err := ctsa.innerChecker.CheckTSA(sh)
	if err != nil {
		return TimedCheckResult{}, err
	}
	tcr := TimedCheckResult{
		T:  n,
		CR: cr,
	}
	ctsa.cache[sh.Instance().Hostname()] = tcr
	return tcr, nil
}
