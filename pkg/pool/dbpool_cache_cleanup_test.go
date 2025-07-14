package pool

import (
	"fmt"
	"testing"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
)

func TestDBPool_CacheCleanupBasic(t *testing.T) {
	// Create shard mapping with Yandex zones
	mapping := map[string]*config.Shard{
		"test_shard": {
			RawHosts: []string{"host1:5432:sas", "host2:5432:klg", "host3:5432:vla"},
		},
	}

	// Create DBPool with short cache age for testing
	dbPool := NewDBPoolFromMultiPool(mapping, nil, &emptyMultiShardPool{}, time.Hour)
	defer dbPool.StopCacheWatchdog()

	// Create a cache with a short max age for testing
	shortCache := NewDbpoolCacheWithCleanup(100*time.Millisecond, DefaultCheckInterval)
	defer shortCache.StopWatchdog()

	// Add some entries to cache
	shortCache.MarkMatched(config.TargetSessionAttrsRW, "host1:5432", "sas", true, "good")
	shortCache.MarkMatched(config.TargetSessionAttrsRO, "host2:5432", "klg", true, "good")
	shortCache.MarkUnmatched(config.TargetSessionAttrsRW, "host3:5432", "vla", false, "bad")
	shortCache.MarkMatched(config.TargetSessionAttrsAny, "host4:5432", "sas", true, "good")

	// Verify initial state
	entries := shortCache.GetAllEntries()
	if len(entries) != 4 {
		t.Errorf("Expected 4 cache entries, got %d", len(entries))
	}

	// Wait for entries to become stale
	time.Sleep(150 * time.Millisecond)

	// Run cleanup manually
	shortCache.cleanupStaleEntries()

	// Check that stale entries were removed
	entries = shortCache.GetAllEntries()
	if len(entries) != 0 {
		t.Errorf("Expected all stale entries to be removed, got %d remaining", len(entries))
	}
}

func TestDBPool_CacheCleanupGoroutine(t *testing.T) {
	// Create a simple test to verify the goroutine starts and stops properly
	mapping := map[string]*config.Shard{
		"test_shard": {
			RawHosts: []string{"host1:5432:sas"},
		},
	}

	dbPool := NewDBPoolFromMultiPool(mapping, nil, &emptyMultiShardPool{}, time.Hour)

	// Verify cache has cleanup functionality
	if dbPool.cache == nil {
		t.Error("Expected cache to be initialized")
	}

	// Stop cleanup
	dbPool.StopCacheWatchdog()

	// Give it a moment to process the cancellation
	time.Sleep(50 * time.Millisecond)

	// Test passes if no panic occurs during cleanup stop
}

func TestCacheCleanupIntegration(t *testing.T) {
	cache := NewDbpoolCache()

	// Test GetAllEntries method
	cache.MarkMatched(config.TargetSessionAttrsRW, "host1:5432", "sas", true, "good")
	cache.MarkUnmatched(config.TargetSessionAttrsRO, "host2:5432", "klg", false, "bad")

	entries := cache.GetAllEntries()
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}
}

// emptyMultiShardPool is a minimal implementation for testing
type emptyMultiShardPool struct{}

func (e *emptyMultiShardPool) ConnectionHost(clid uint, shardKey kr.ShardKey, host config.Host) (shard.ShardHostInstance, error) {
	return nil, fmt.Errorf("test pool - no connections available")
}
func (e *emptyMultiShardPool) SetRule(rule *config.BackendRule)                    {}
func (e *emptyMultiShardPool) ForEach(cb func(sh shard.ShardHostInfo) error) error { return nil }
func (e *emptyMultiShardPool) Put(sh shard.ShardHostInstance) error                { return nil }
func (e *emptyMultiShardPool) Discard(sh shard.ShardHostInstance) error            { return nil }
func (e *emptyMultiShardPool) ForEachPool(cb func(pool Pool) error) error          { return nil }
func (e *emptyMultiShardPool) View() Statistics                                    { return Statistics{} }
func (e *emptyMultiShardPool) ID() uint                                            { return 0 }
