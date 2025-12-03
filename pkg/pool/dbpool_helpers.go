package pool

import (
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

/*
* These methods used for testing only
 */

// TODO: add shuffle host support here
func NewDBPoolFromMultiPool(mapping map[string]*config.Shard, sp *startup.StartupParams, mp ShardHostsPool, tsaRecheckDuration time.Duration) *DBPool {
	dbPool := &DBPool{
		pool:              mp,
		shardMapping:      mapping,
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
func NewDBPoolWithAllocator(mapping map[string]*config.Shard, startupParams *startup.StartupParams, allocator ConnectionAllocFn) *DBPool {
	dbPool := &DBPool{
		pool:              NewPool(allocator),
		shardMapping:      mapping,
		checker:           tsa.NewCachedTSAChecker(),
		deadCheckInterval: 0, // Disabled for testing
		AcquireRetryCount: 1,
		recheckTCP:        false,
	}

	// Create cache with cleanup functionality (5 minute max age)
	dbPool.cache = NewDbpoolCacheWithCleanup(DefaultCacheTTL, DefaultCheckInterval)

	return dbPool
}
