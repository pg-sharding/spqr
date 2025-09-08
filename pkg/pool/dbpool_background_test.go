package pool

import (
	"testing"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/tsa"
)


func TestDBPool_BackgroundHealthCheck(t *testing.T) {
	// Create shard mapping
	mapping := map[string]*config.Shard{
		"test_shard": {
			RawHosts: []string{"host1:5432:sas", "host2:5432:klg"},
		},
	}

	// Create DBPool with background health checking enabled (short interval for test)
	dbPool := &DBPool{
		pool:              &emptyMultiShardPool{},
		shardMapping:      mapping,
		checker:           tsa.NewCachedTSAChecker(),
		deadCheckInterval: 50 * time.Millisecond, // Very short for testing
	}

	// Create cache
	dbPool.cache = NewDbpoolCacheWithCleanup(time.Hour, time.Hour)
	defer dbPool.StopCacheWatchdog()

	// Add a dead host to cache
	dbPool.cache.MarkUnmatched(config.TargetSessionAttrsRW, "host1:5432", "sas", false, "connection failed")

	// Start background health checking
	dbPool.StartBackgroundHealthCheck()
	defer dbPool.StopBackgroundHealthCheck()

	// Wait a bit for background checking to potentially run
	time.Sleep(100 * time.Millisecond)

	// Test passes if no panics occur
}

func TestDBPool_BackgroundHealthCheckDisabled(t *testing.T) {
	// Create shard mapping
	mapping := map[string]*config.Shard{
		"test_shard": {
			RawHosts: []string{"host1:5432:sas"},
		},
	}

	// Create DBPool with background health checking disabled
	dbPool := &DBPool{
		pool:              &emptyMultiShardPool{},
		shardMapping:      mapping,
		checker:           tsa.NewCachedTSAChecker(),
		deadCheckInterval: 0, // Disabled
	}

	// Create cache
	dbPool.cache = NewDbpoolCacheWithCleanup(time.Hour, time.Hour)
	defer dbPool.StopCacheWatchdog()

	// Start background health checking (should be a no-op)
	dbPool.StartBackgroundHealthCheck()
	defer dbPool.StopBackgroundHealthCheck()

	// Test passes if no panics occur and no background checking starts
}

func TestDBPool_EvaluateTSAMatch(t *testing.T) {
	dbPool := &DBPool{}

	tests := []struct {
		name        string
		result      tsa.CheckResult
		requiredTSA tsa.TSA
		expected    bool
	}{
		{
			name:        "RW host matches RW requirement",
			result:      tsa.CheckResult{Alive: true, RW: true},
			requiredTSA: config.TargetSessionAttrsRW,
			expected:    true,
		},
		{
			name:        "RO host matches RO requirement",
			result:      tsa.CheckResult{Alive: true, RW: false},
			requiredTSA: config.TargetSessionAttrsRO,
			expected:    true,
		},
		{
			name:        "RW host does not match RO requirement",
			result:      tsa.CheckResult{Alive: true, RW: true},
			requiredTSA: config.TargetSessionAttrsRO,
			expected:    false,
		},
		{
			name:        "Dead host matches nothing",
			result:      tsa.CheckResult{Alive: false, RW: true},
			requiredTSA: config.TargetSessionAttrsRW,
			expected:    false,
		},
		{
			name:        "Any host matches ANY requirement",
			result:      tsa.CheckResult{Alive: true, RW: true},
			requiredTSA: config.TargetSessionAttrsAny,
			expected:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := dbPool.evaluateTSAMatch(tt.result, tt.requiredTSA)
			if actual != tt.expected {
				t.Errorf("evaluateTSAMatch() = %v, expected %v", actual, tt.expected)
			}
		})
	}
}

// Remove the mock checker since we're using the real one
