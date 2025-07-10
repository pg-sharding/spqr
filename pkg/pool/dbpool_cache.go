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
	maxAge time.Duration // Maximum age for cache entries before they're considered stale
}

// NewDbpoolCache creates a new cache instance
func NewDbpoolCache() *DbpoolCache {
	return &DbpoolCache{
		cache:  &sync.Map{},
		maxAge: 5 * time.Minute, // Default max age for cache entries
	}
}

// NewDbpoolCacheWithCleanup creates a new cache instance with automatic cleanup
func NewDbpoolCacheWithCleanup(maxAge time.Duration) *DbpoolCache {
	ctx, cancel := context.WithCancel(context.Background())

	cache := &DbpoolCache{
		cache:         &sync.Map{},
		cleanupCtx:    ctx,
		cleanupCancel: cancel,
		maxAge:        maxAge,
	}

	// Start the cleanup goroutine
	cache.startCacheCleanup()

	return cache
}

// MarkGood marks a host as good (alive and suitable for the requested TSA)
func (c *DbpoolCache) MarkGood(targetSessionAttrs tsa.TSA, host, az string, alive bool, reason string) {
	key := TsaKey{
		Tsa:  targetSessionAttrs,
		Host: host,
		AZ:   az,
	}

	result := LocalCheckResult{
		Alive:  alive,
		Good:   true,
		Reason: reason,
	}

	entry := CachedEntry{
		Result:        result,
		LastCheckTime: time.Now(),
	}

	c.cache.Store(key, entry)
}

// MarkBad marks a host as bad (alive but not suitable, or not alive)
func (c *DbpoolCache) MarkBad(targetSessionAttrs tsa.TSA, host, az string, alive bool, reason string) {
	key := TsaKey{
		Tsa:  targetSessionAttrs,
		Host: host,
		AZ:   az,
	}

	result := LocalCheckResult{
		Alive:  alive,
		Good:   false,
		Reason: reason,
	}

	entry := CachedEntry{
		Result:        result,
		LastCheckTime: time.Now(),
	}

	c.cache.Store(key, entry)
}

// Good checks if a host is marked as good for the given TSA
// Returns the check result and whether the entry exists in cache and is not stale
func (c *DbpoolCache) Good(targetSessionAttrs tsa.TSA, host, az string) (LocalCheckResult, bool) {
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
	if time.Since(entry.LastCheckTime) > c.maxAge {
		c.cache.Delete(key) // Remove stale entry
		return LocalCheckResult{}, false
	}

	return entry.Result, true
}

// Bad checks if a host is marked as bad for the given TSA
// This is essentially the inverse of Good, but provided for clarity
func (c *DbpoolCache) Bad(targetSessionAttrs tsa.TSA, host, az string) (LocalCheckResult, bool) {
	result, exists := c.Good(targetSessionAttrs, host, az)
	if !exists {
		return LocalCheckResult{}, false
	}

	return result, !result.Good
}

// Clear removes all cached entries
func (c *DbpoolCache) Clear() {
	c.cache.Range(func(key, value interface{}) bool {
		c.cache.Delete(key)
		return true
	})
}

// Remove removes a specific cached entry
func (c *DbpoolCache) Remove(targetSessionAttrs tsa.TSA, host, az string) {
	key := TsaKey{
		Tsa:  targetSessionAttrs,
		Host: host,
		AZ:   az,
	}
	c.cache.Delete(key)
}

// ReplaceCache replaces the internal cache with a new sync.Map for testing purposes
func (c *DbpoolCache) ReplaceCache(newCache *sync.Map) {
	c.cache = newCache
}

// GetAllEntries returns all cache entries for validation purposes
func (c *DbpoolCache) GetAllEntries() map[TsaKey]LocalCheckResult {
	entries := make(map[TsaKey]LocalCheckResult)
	c.cache.Range(func(key, value interface{}) bool {
		if tsaKey, ok := key.(TsaKey); ok {
			if entry, ok := value.(CachedEntry); ok {
				entries[tsaKey] = entry.Result
			}
		}
		return true
	})
	return entries
}

// RemoveByKey removes a specific cache entry by TsaKey
func (c *DbpoolCache) RemoveByKey(key TsaKey) {
	c.cache.Delete(key)
}

// startCacheCleanup starts a background goroutine that cleans up stale cache entries every 30 seconds
func (c *DbpoolCache) startCacheCleanup() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-c.cleanupCtx.Done():
				spqrlog.Zero.Info().Msg("cache cleanup goroutine stopped")
				return
			case <-ticker.C:
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

	c.cache.Range(func(key, value interface{}) bool {
		totalCount++

		if entry, ok := value.(CachedEntry); ok {
			if now.Sub(entry.LastCheckTime) > c.maxAge {
				c.cache.Delete(key)
				removedCount++

				if tsaKey, ok := key.(TsaKey); ok {
					spqrlog.Zero.Debug().
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
		Dur("max_age", c.maxAge).
		Msg("cache cleanup completed")
}

// StopCleanup stops the cache cleanup goroutine
func (c *DbpoolCache) StopCleanup() {
	if c.cleanupCancel != nil {
		c.cleanupCancel()
	}
}
