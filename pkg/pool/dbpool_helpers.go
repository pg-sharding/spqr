package pool

import (
	"time"

	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

/*
* These methods used for testing only
 */

// TODO: add shuffle host support here
func NewDBPoolFromMultiPool(tmgr topology.TopologyMgr, _ *startup.StartupParams, mp ShardHostsPool, tsaRecheckDuration time.Duration) *DBPool {
	dbPool := &DBPool{
		pool:              mp,
		tmgr:              tmgr,
		checker:           tsa.NewCachedTSACheckerWithDuration(tsaRecheckDuration),
		deadCheckInterval: 0, // Disabled for testing
		AcquireRetryCount: 1,
		recheckTCP:        false,
	}

	// Create cache with cleanup functionality (5 minute max age)
	dbPool.cache = NewDbpoolCacheWithCleanup(DefaultCacheTTL, DefaultCheckInterval)

	return dbPool
}

// TODO: add shuffle host support here
func NewDBPoolWithAllocator(tmgr topology.TopologyMgr, _ *startup.StartupParams, allocator ConnectionAllocFn) *DBPool {
	dbPool := &DBPool{
		pool:              NewPool(allocator),
		tmgr:              tmgr,
		checker:           tsa.NewCachedTSAChecker(),
		deadCheckInterval: 0, // Disabled for testing
		AcquireRetryCount: 1,
		recheckTCP:        false,
	}

	// Create cache with cleanup functionality (5 minute max age)
	dbPool.cache = NewDbpoolCacheWithCleanup(DefaultCacheTTL, DefaultCheckInterval)

	return dbPool
}
