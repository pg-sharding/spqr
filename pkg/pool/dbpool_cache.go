package pool

import (
	"context"
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

// CachedEntry represents a cached TSA check result with timestamp
type CachedEntry struct {
	Result        LocalCheckResult
	LastCheckTime time.Time
}

// DbpoolCache manages caching of TSA (Target Session Attributes) check results
// It provides efficient lookup and storage for host health information with automatic time-based cleanup
type DbpoolCache struct {
	cache *sync.Map

	// Cleanup management
	cleanupCtx    context.Context
	cleanupCancel context.CancelFunc

	// Configuration
	cacheTTL time.Duration // Maximum age for cache entries before they're considered stale
}

const (
	defaultCacheTTL      = 5 * time.Minute
	DefaultCheckInterval = 30 * time.Second
)

// NewDbpoolCache creates a new cache instance
func NewDbpoolCache() *DbpoolCache {
	return &DbpoolCache{
		cache:    &sync.Map{},
		cacheTTL: defaultCacheTTL, // Default max age for cache entries
	}
}

// NewDbpoolCacheWithCleanup creates a new cache instance with automatic cleanup
func NewDbpoolCacheWithCleanup(cacheTTL time.Duration, healthCheckInterval time.Duration) *DbpoolCache {

	cache := &DbpoolCache{
		cache:    &sync.Map{},
		cacheTTL: cacheTTL,
	}

	if healthCheckInterval > time.Duration(0) {
		// Start the cleanup goroutine
		ctx, cancel := context.WithCancel(context.Background())

		cache.cleanupCancel = cancel
		cache.cleanupCtx = ctx

		cache.startCacheCleanup(healthCheckInterval)
	}

	return cache
}

// MarkMatched marks a host as good (alive and suitable for the requested TSA)
func (c *DbpoolCache) MarkMatched(targetSessionAttrs tsa.TSA, host, az string, alive bool, reason string) {
	key := TsaKey{
		Tsa:  targetSessionAttrs,
		Host: host,
		AZ:   az,
	}

	result := LocalCheckResult{
		Alive:  alive,
		Match:  true,
		Reason: reason,
	}

	entry := CachedEntry{
		Result:        result,
		LastCheckTime: time.Now(),
	}

	c.cache.Store(key, entry)
}

// MarkUnmatched marks a host as bad (alive but not suitable, or not alive)
func (c *DbpoolCache) MarkUnmatched(targetSessionAttrs tsa.TSA, host, az string, alive bool, reason string) {
	key := TsaKey{
		Tsa:  targetSessionAttrs,
		Host: host,
		AZ:   az,
	}

	result := LocalCheckResult{
		Alive:  alive,
		Match:  false,
		Reason: reason,
	}

	entry := CachedEntry{
		Result:        result,
		LastCheckTime: time.Now(),
	}

	c.cache.Store(key, entry)
}

// Match checks if a host is marked as matched for the given TSA
// Returns the check result and whether the entry exists in cache and is not stale
func (c *DbpoolCache) Match(targetSessionAttrs tsa.TSA, host, az string) (LocalCheckResult, bool) {
	key := TsaKey{
		Tsa:  targetSessionAttrs,
		Host: host,
		AZ:   az,
	}

	value, exists := c.cache.Load(key)
	if !exists {
		return LocalCheckResult{}, false
	}

	entry := value.(CachedEntry)

	// Check if entry is stale
	if time.Since(entry.LastCheckTime) > c.cacheTTL {
		// This is a stale entry, return dummy response
		return LocalCheckResult{}, false
	}

	return entry.Result, true
}

// Clear removes all cached entries
func (c *DbpoolCache) Clear() {
	c.cache.Range(func(key, value any) bool {
		c.cache.Delete(key)
		return true
	})
}

// GetAllEntries returns all cache entries for validation purposes
func (c *DbpoolCache) GetAllEntries() map[TsaKey]LocalCheckResult {
	entries := make(map[TsaKey]LocalCheckResult)
	c.cache.Range(func(key, value any) bool {
		if tsaKey, ok := key.(TsaKey); ok {
			if entry, ok := value.(CachedEntry); ok {
				entries[tsaKey] = entry.Result
			}
		}
		return true
	})
	return entries
}

// startCacheCleanup starts a background goroutine that cleans up stale cache entries every 30 seconds
func (c *DbpoolCache) startCacheCleanup(d time.Duration) {
	go func() {
		ticker := time.NewTicker(d)
		defer ticker.Stop()

		for {
			select {
			case <-c.cleanupCtx.Done():
				spqrlog.Zero.Info().Msg("cache cleanup goroutine stopped")
				return
			case <-ticker.C:
				/* do real work */
				c.cleanupStaleEntries()
			}
		}
	}()
}

// cleanupStaleEntries removes entries that are older than maxAge
func (c *DbpoolCache) cleanupStaleEntries() {
	now := time.Now()
	removedCount := 0
	totalCount := 0

	c.cache.Range(func(key, value any) bool {
		totalCount++

		if entry, ok := value.(CachedEntry); ok {
			if now.Sub(entry.LastCheckTime) > c.cacheTTL {
				c.cache.Delete(key)
				removedCount++

				if tsaKey, ok := key.(TsaKey); ok {
					spqrlog.Zero.Info().
						Str("host", tsaKey.Host).
						Str("az", tsaKey.AZ).
						Str("tsa", string(tsaKey.Tsa)).
						Dur("age", now.Sub(entry.LastCheckTime)).
						Msg("removed stale cache entry")
				}
			}
		}
		return true
	})

	spqrlog.Zero.Debug().
		Int("removed_entries", removedCount).
		Int("total_entries", totalCount).
		Int("remaining_entries", totalCount-removedCount).
		Dur("max_age", c.cacheTTL).
		Msg("cache cleanup completed")
}

// StopWatchdog stops the cache cleanup goroutine
func (c *DbpoolCache) StopWatchdog() {
	if c.cleanupCancel != nil {
		c.cleanupCancel()
	}
}
