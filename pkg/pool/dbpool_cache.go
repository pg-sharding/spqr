package pool

import (
	"sync"

	"github.com/pg-sharding/spqr/pkg/tsa"
)

// DbpoolCache manages caching of TSA (Target Session Attributes) check results
// It provides efficient lookup and storage for host health information
type DbpoolCache struct {
	cache *sync.Map
}

// NewDbpoolCache creates a new cache instance
func NewDbpoolCache() *DbpoolCache {
	return &DbpoolCache{
		cache: &sync.Map{},
	}
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

	c.cache.Store(key, result)
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

	c.cache.Store(key, result)
}

// Good checks if a host is marked as good for the given TSA
// Returns the check result and whether the entry exists in cache
func (c *DbpoolCache) Good(targetSessionAttrs tsa.TSA, host, az string) (LocalCheckResult, bool) {
	key := TsaKey{
		Tsa:  targetSessionAttrs,
		Host: host,
		AZ:   az,
	}

	result, exists := c.cache.Load(key)
	if !exists {
		return LocalCheckResult{}, false
	}

	checkResult := result.(LocalCheckResult)
	return checkResult, true
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
