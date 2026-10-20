package tsa

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/shard"
)

type CachedTSAChecker struct {
	recheckPeriod time.Duration
	cache         sync.Map
	innerChecker  *NetChecker
}

type observationKey struct {
	shard string
	host  string
	az    string
}

// InstanceHealthChecks aggregates each host's newest scoped observation.
func (ctsa *CachedTSAChecker) InstanceHealthChecks() map[string]CachedCheckResult {
	results := map[string]CachedCheckResult{}
	ctsa.cache.Range(func(k, v any) bool {
		host := k.(observationKey).host
		result := v.(CachedCheckResult)
		if old, ok := results[host]; !ok || result.LastCheckTime.After(old.LastCheckTime) {
			results[host] = result
		}
		return true
	})
	return results
}

const defaultTSARecheckPeriod = time.Second

func NewCachedTSAChecker() *CachedTSAChecker {
	return NewCachedTSACheckerWithDuration(config.ValueOrDefaultDuration(config.RouterConfig().DbpoolTSARecheckPeriod, defaultTSARecheckPeriod))
}

func NewCachedTSACheckerWithDuration(tsaRecheckDuration time.Duration) *CachedTSAChecker {
	return &CachedTSAChecker{recheckPeriod: tsaRecheckDuration, innerChecker: &NetChecker{}}
}

// CheckTSA preserves the standalone checker API. Pools use ObserveTSA and then
// publish through their shared health and routing publication path.
func (ctsa *CachedTSAChecker) CheckTSA(sh shard.ShardHostInstance, timeout time.Duration) (CachedCheckResult, error) {
	host := sh.Instance().Hostname()
	result, err := ctsa.ObserveTSA("", host, "", sh, timeout)
	ctsa.PublishScoped("", host, "", result)
	return result, err
}

// ObserveTSA returns a cached observation or performs a network check without
// publishing it. Its owner can reject obsolete generations before publishing.
func (ctsa *CachedTSAChecker) ObserveTSA(shardName, host, az string, sh shard.ShardHostInstance, timeout time.Duration) (CachedCheckResult, error) {
	if result, ok := ctsa.CachedScoped(shardName, host, az); ok {
		if !result.CR.Alive {
			return result, fmt.Errorf("%s", result.CR.Reason)
		}
		return result, nil
	}
	return ctsa.CheckTSAUncached(sh, timeout)
}

// CachedScoped reads a fresh observation without beginning a network attempt.
func (ctsa *CachedTSAChecker) CachedScoped(shardName, host, az string) (CachedCheckResult, bool) {
	if v, ok := ctsa.cache.Load(observationKey{shardName, host, az}); ok {
		result := v.(CachedCheckResult)
		// Jitter expiry so foreground checks do not synchronize on a fixed TTL.
		if float64(time.Since(result.LastCheckTime)) < (1+rand.Float64())*float64(ctsa.recheckPeriod) {
			return result, true
		}
	}
	return CachedCheckResult{}, false
}

// CheckTSAUncached performs a real SQL attempt without publication. Its caller
// assigns ordering before invoking it and accepts the result after completion.
func (ctsa *CachedTSAChecker) CheckTSAUncached(sh shard.ShardHostInstance, timeout time.Duration) (CachedCheckResult, error) {
	now := time.Now()
	cr, err := ctsa.innerChecker.CheckTSA(sh, timeout)
	result := CachedCheckResult{LastCheckTime: now, CR: cr}
	if err != nil {
		result.CR.Reason = err.Error()
		result.FailureStage = "query"
	} else {
		result.LastSuccessTime = now
	}
	return result, err
}

// PublishScoped replaces only older attempts for this route's shard, host, and
// availability zone. Failed observations replace stale successful observations.
func (ctsa *CachedTSAChecker) PublishScoped(shardName, host, az string, result CachedCheckResult) bool {
	key := observationKey{shardName, host, az}
	for {
		previous, ok := ctsa.cache.Load(key)
		if !ok {
			if _, loaded := ctsa.cache.LoadOrStore(key, result); !loaded {
				return true
			}
			continue
		}
		old := previous.(CachedCheckResult)
		if result.LastCheckTime.Before(old.LastCheckTime) {
			return false
		}
		if result.LastSuccessTime.Before(old.LastSuccessTime) {
			result.LastSuccessTime = old.LastSuccessTime
		}
		if ctsa.cache.CompareAndSwap(key, previous, result) {
			return true
		}
	}
}

// RemoveScoped forgets an obsolete target. The owner rejects any in-flight
// results before publication so removed entries cannot be resurrected.
func (ctsa *CachedTSAChecker) RemoveScoped(shardName, host, az string) {
	ctsa.cache.Delete(observationKey{shardName, host, az})
}
