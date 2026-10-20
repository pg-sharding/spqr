package tsa_test

import (
	"testing"
	"time"

	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/stretchr/testify/require"
)

func TestScopedPublicationRetainsFailuresAndNewestObservation(t *testing.T) {
	checker := tsa.NewCachedTSACheckerWithDuration(time.Hour)
	now := time.Now()
	success := tsa.CachedCheckResult{CR: tsa.CheckResult{Alive: true, RW: true}, LastCheckTime: now, LastSuccessTime: now}
	require.True(t, checker.PublishScoped("shard1", "host", "az1", success))
	stored, ok := checker.CachedScoped("shard1", "host", "az1")
	require.True(t, ok)
	require.Equal(t, success, stored)
	_, ok = checker.CachedScoped("shard1", "host", "another-az")
	require.False(t, ok)
	failure := tsa.CachedCheckResult{CR: tsa.CheckResult{Reason: "authentication rejected"}, LastCheckTime: now.Add(time.Second), FailureStage: "authentication"}
	require.True(t, checker.PublishScoped("shard1", "host", "az1", failure))
	require.False(t, checker.PublishScoped("shard1", "host", "az1", success))
	result := checker.InstanceHealthChecks()["host"]
	require.False(t, result.CR.Alive)
	require.Equal(t, "authentication", result.FailureStage)
	require.Equal(t, now, result.LastSuccessTime)
	// A different shard and AZ has independent state; host aggregation still
	// shows the most recent attempt, regardless of sync.Map iteration order.
	require.True(t, checker.PublishScoped("shard2", "host", "az2", success))
	require.False(t, checker.InstanceHealthChecks()["host"].CR.Alive)
	checker.RemoveScoped("shard1", "host", "az1")
	require.True(t, checker.InstanceHealthChecks()["host"].CR.RW)
	checker.RemoveScoped("shard2", "host", "az2")
	require.Empty(t, checker.InstanceHealthChecks())
	expired := success
	expired.LastCheckTime = now.Add(-3 * time.Hour)
	require.True(t, checker.PublishScoped("shard1", "host", "az1", expired))
	_, ok = checker.CachedScoped("shard1", "host", "az1")
	require.False(t, ok)
}
