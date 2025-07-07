package tsa

import (
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/shard"
)

type CachedTSAChecker struct {
	mu            sync.Mutex
	recheckPeriod time.Duration
	cache         map[string]CachedCheckResult
	innerChecker  *NetChecker
}

// InstanceHealthChecks implements CachedTSAChecker.
func (ctsa *CachedTSAChecker) InstanceHealthChecks() map[string]CachedCheckResult {
	ctsa.mu.Lock()
	defer ctsa.mu.Unlock()

	cp := map[string]CachedCheckResult{}
	for k, v := range ctsa.cache {
		cp[k] = v
	}

	return cp
}

// NewTSAChecker creates a new instance of TSAChecker.
// It returns a TSAChecker interface that can be used to perform TSA checks.
//
// Returns:
//   - TSAChecker: A new instance of TSAChecker.
func NewCachedTSAChecker() *CachedTSAChecker {
	return &CachedTSAChecker{
		mu:            sync.Mutex{},
		recheckPeriod: time.Second,
		cache:         map[string]CachedCheckResult{},
		innerChecker:  &NetChecker{},
	}
}

func NewCachedTSACheckerWithDuration(tsaRecheckDuration time.Duration) *CachedTSAChecker {
	return &CachedTSAChecker{
		mu:            sync.Mutex{},
		recheckPeriod: tsaRecheckDuration,
		cache:         map[string]CachedCheckResult{},
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
func (ctsa *CachedTSAChecker) CheckTSA(sh shard.ShardHostInstance) (CachedCheckResult, error) {
	ctsa.mu.Lock()
	defer ctsa.mu.Unlock()

	n := time.Now()
	if e, ok := ctsa.cache[sh.Instance().Hostname()]; ok && n.UnixNano()-e.LastCheckTime.UnixNano() < ctsa.recheckPeriod.Nanoseconds() {
		return e, nil
	}

	cr, err := ctsa.innerChecker.CheckTSA(sh)
	if err != nil {
		return CachedCheckResult{}, err
	}
	tcr := CachedCheckResult{
		LastCheckTime: n,
		CR:            cr,
	}
	ctsa.cache[sh.Instance().Hostname()] = tcr
	return tcr, nil
}
