package tsa

import (
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/shard"
)

type cacheEntry struct {
	result    CheckResult
	lastCheck int64
}

type CachedTSAChecker struct {
	mu            sync.Mutex
	recheckPeriod time.Duration
	cache         map[string]cacheEntry
	innerChecker  TSAChecker
}

var _ TSAChecker = (*CachedTSAChecker)(nil)
var _ CachedResultsGetter = (*CachedTSAChecker)(nil)

// NewTSAChecker creates a new instance of TSAChecker.
// It returns a TSAChecker interface that can be used to perform TSA checks.
//
// Returns:
//   - TSAChecker: A new instance of TSAChecker.
func NewTSAChecker() *CachedTSAChecker {
	return &CachedTSAChecker{
		mu:            sync.Mutex{},
		recheckPeriod: time.Second,
		cache:         map[string]cacheEntry{},
		innerChecker:  &NetChecker{},
	}
}

func NewTSACheckerWithDuration(tsaRecheckDuration time.Duration) *CachedTSAChecker {
	return &CachedTSAChecker{
		mu:            sync.Mutex{},
		recheckPeriod: tsaRecheckDuration,
		cache:         map[string]cacheEntry{},
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
func (ctsa *CachedTSAChecker) CheckTSA(sh shard.Shard) (CheckResult, error) {
	ctsa.mu.Lock()
	defer ctsa.mu.Unlock()

	n := time.Now().UnixNano()
	if e, ok := ctsa.cache[sh.Instance().Hostname()]; ok && n-e.lastCheck < ctsa.recheckPeriod.Nanoseconds() {
		return e.result, nil
	}

	cr, err := ctsa.innerChecker.CheckTSA(sh)
	if err != nil {
		return cr, err
	}
	ctsa.cache[sh.Instance().Hostname()] = cacheEntry{
		lastCheck: n,
		result:    cr,
	}
	return cr, nil
}

// GetCachedResults returns all cached check results for shards.
//
// Returns:
//   - map[string]CheckResult: A map of hostname to CheckResult for all cached entries.
func (ctsa *CachedTSAChecker) GetCachedResults() map[string]CachedCheckResult {
	ctsa.mu.Lock()
	defer ctsa.mu.Unlock()

	results := make(map[string]CachedCheckResult)
	for hostname, entry := range ctsa.cache {
		results[hostname] = CachedCheckResult{
			Hostname: hostname,
			Result:   entry.result,
		}
	}
	return results
}
