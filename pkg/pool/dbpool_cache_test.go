package pool

import (
	"sync"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

// Bad checks if a host is marked as bad for the given TSA
// This is essentially the inverse of Match, but provided for clarity, currecntly used only
// for testing purposes
func (c *DbpoolCache) Bad(targetSessionAttrs tsa.TSA, host, az string) (LocalCheckResult, bool) {
	result, exists := c.Match(targetSessionAttrs, host, az)
	if !exists {
		return LocalCheckResult{}, false
	}

	return result, !result.Match
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

func TestDbpoolCache_MarkGoodAndBad(t *testing.T) {
	cache := NewDbpoolCache()

	// Test marking a host as good
	cache.MarkMatched(config.TargetSessionAttrsRW, "localhost:5432", "sas", true, "connection successful")

	result, exists := cache.Match(config.TargetSessionAttrsRW, "localhost:5432", "sas")
	if !exists {
		t.Error("Expected cache entry to exist")
	}
	if !result.Match {
		t.Error("Expected host to be marked as good")
	}
	if !result.Alive {
		t.Error("Expected host to be alive")
	}
	if result.Reason != "connection successful" {
		t.Errorf("Expected reason 'connection successful', got '%s'", result.Reason)
	}

	// Test marking a host as bad
	cache.MarkUnmatched(config.TargetSessionAttrsRO, "localhost:5433", "klg", false, "connection failed")

	result, exists = cache.Match(config.TargetSessionAttrsRO, "localhost:5433", "klg")
	if !exists {
		t.Error("Expected cache entry to exist")
	}
	if result.Match {
		t.Error("Expected host to be marked as bad")
	}
	if result.Alive {
		t.Error("Expected host to be dead")
	}
	if result.Reason != "connection failed" {
		t.Errorf("Expected reason 'connection failed', got '%s'", result.Reason)
	}

	// Test Bad method
	badResult, isBad := cache.Bad(config.TargetSessionAttrsRO, "localhost:5433", "klg")
	if !isBad {
		t.Error("Expected host to be bad")
	}
	if badResult.Match {
		t.Error("Expected bad result to have Good=false")
	}
}

func TestDbpoolCache_NonExistentEntry(t *testing.T) {
	cache := NewDbpoolCache()

	// Test querying non-existent entry
	_, exists := cache.Match(config.TargetSessionAttrsAny, "nonexistent:5432", "unknown")
	if exists {
		t.Error("Expected cache entry to not exist")
	}

	_, isBad := cache.Bad(config.TargetSessionAttrsAny, "nonexistent:5432", "unknown")
	if isBad {
		t.Error("Expected non-existent entry to not be bad")
	}
}

func TestDbpoolCache_Clear(t *testing.T) {
	cache := NewDbpoolCache()

	// Add some entries
	cache.MarkMatched(config.TargetSessionAttrsRW, "host1:5432", "sas", true, "good")
	cache.MarkUnmatched(config.TargetSessionAttrsRO, "host2:5432", "vla", false, "bad")

	// Clear cache
	cache.Clear()

	// Verify entries are gone
	_, exists1 := cache.Match(config.TargetSessionAttrsRW, "host1:5432", "sas")
	_, exists2 := cache.Match(config.TargetSessionAttrsRO, "host2:5432", "vla")
	if exists1 || exists2 {
		t.Error("Expected cache entries to not exist after clear")
	}
}

func TestDbpoolCache_Remove(t *testing.T) {
	cache := NewDbpoolCache()

	// Add entries
	cache.MarkMatched(config.TargetSessionAttrsRW, "host1:5432", "sas", true, "good")
	cache.MarkUnmatched(config.TargetSessionAttrsRO, "host2:5432", "klg", false, "bad")

	// Remove one entry
	cache.Remove(config.TargetSessionAttrsRW, "host1:5432", "sas")

	// Verify first entry is gone, second still exists
	_, exists1 := cache.Match(config.TargetSessionAttrsRW, "host1:5432", "sas")
	_, exists2 := cache.Match(config.TargetSessionAttrsRO, "host2:5432", "klg")

	if exists1 {
		t.Error("Expected removed entry to not exist")
	}
	if !exists2 {
		t.Error("Expected non-removed entry to still exist")
	}
}
