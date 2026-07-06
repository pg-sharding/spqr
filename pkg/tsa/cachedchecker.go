package tsa

import (
	"math/rand/v2"
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/shard"
)

type CachedTSAChecker struct {
	recheckPeriod time.Duration
	cache         sync.Map
	innerChecker  *NetChecker
}

// InstanceHealthChecks implements CachedTSAChecker.
func (ctsa *CachedTSAChecker) InstanceHealthChecks() map[string]CachedCheckResult {

	cp := map[string]CachedCheckResult{}
	ctsa.cache.Range(func(k, v any) bool {
		cp[k.(string)] = v.(CachedCheckResult)
		return true
	})

	return cp
}

// NewTSAChecker creates a new instance of TSAChecker.
// It returns a TSAChecker interface that can be used to perform TSA checks.
//
// Returns:
//   - TSAChecker: A new instance of TSAChecker.
func NewCachedTSAChecker() *CachedTSAChecker {
	return &CachedTSAChecker{
		recheckPeriod: time.Second,
		cache:         sync.Map{},
		innerChecker:  &NetChecker{},
	}
}

func NewCachedTSACheckerWithDuration(tsaRecheckDuration time.Duration) *CachedTSAChecker {
	return &CachedTSAChecker{
		recheckPeriod: tsaRecheckDuration,
		cache:         sync.Map{},
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

	n := time.Now()
	if v, ok := ctsa.cache.Load(sh.Instance().Hostname()); ok {
		e := v.(CachedCheckResult)
		dff := n.UnixNano() - e.LastCheckTime.UnixNano()
		/* Randomize expiration time here to avoid "check spike" */
		coef := 1 + rand.Float64()
		if float64(dff) < coef*float64(ctsa.recheckPeriod.Nanoseconds()) {
			return e, nil
		}
	}

	/* There is a room for race, where two (or more) checkers
	* will end up running innerChecker.
	* However, a concurrent protocol to avoid this is too
	* much for troubles here, so we are fine. */
	cr, err := ctsa.innerChecker.CheckTSA(sh)
	if err != nil {
		return CachedCheckResult{}, err
	}
	tcr := CachedCheckResult{
		LastCheckTime: n,
		CR:            cr,
	}

	ctsa.cache.Store(sh.Instance().Hostname(), tcr)
	return tcr, nil
}
