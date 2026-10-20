package pool

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func monitoredTestPool(t *testing.T, hosts map[string][]string) *DBPool {
	t.Helper()
	mapping := map[string]*config.Shard{}
	for name, addresses := range hosts {
		mapping[name] = &config.Shard{RawHosts: addresses, Type: config.DataShard}
	}
	p := NewDBPool(topology.TopMgrFromMap(topology.DataShardMapFromConfig(mapping)), &startup.StartupParams{}, "", time.Hour, 0).(*DBPool)
	p.deadCheckInterval = 0
	p.ShuffleHosts = false
	p.SetRule(&config.BackendRule{Usr: "user", DB: "database"})
	t.Cleanup(p.StopCacheWatchdog)
	return p
}
func awaitPrimary(t *testing.T, p *DBPool, host string, predicate func(tsa.CachedCheckResult) bool) {
	t.Helper()
	require.Eventually(t, func() bool { value, ok := p.InstanceHealthChecks()[host]; return ok && predicate(value) }, time.Second, time.Millisecond)
}
func writableProbe(context.Context, topology.TopologyMgr, *startup.StartupParams, *config.BackendRule, kr.ShardKey, config.Host) (tsa.CheckResult, string, error) {
	return tsa.CheckResult{Alive: true, RW: true, Reason: "primary"}, "", nil
}
func TestPrimaryMonitorDiscoveryPeriodicAndCoalescing(t *testing.T) {
	p := monitoredTestPool(t, map[string][]string{"shard": {"host:1"}})
	m := NewPrimaryMonitor(time.Hour, time.Second, 1)
	defer m.Close()
	entered := make(chan struct{}, 8)
	release := make(chan struct{}, 8)
	m.probe = func(ctx context.Context, _ topology.TopologyMgr, _ *startup.StartupParams, _ *config.BackendRule, _ kr.ShardKey, _ config.Host) (tsa.CheckResult, string, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return tsa.CheckResult{Alive: true, RW: true, Reason: "primary"}, "", nil
		case <-ctx.Done():
			return tsa.CheckResult{}, "query", ctx.Err()
		}
	}
	m.Register(p)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no empty-cache discovery")
	}
	for range 100 {
		m.Request(p, "shard")
	}
	release <- struct{}{}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("early requests were lost")
	}
	release <- struct{}{}
	awaitPrimary(t, p, "host:1", func(v tsa.CachedCheckResult) bool { return v.CR.RW })
	require.Eventually(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return !m.targets[p]["shard"].running }, time.Second, time.Millisecond)
	m.mu.Lock()
	target := m.targets[p]["shard"]
	target.due = time.Now()
	m.mu.Unlock()
	m.signal()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("periodic probe reused foreground cache")
	}
	release <- struct{}{}
}

func TestPrimaryMonitorDemotionOutageAndRecovery(t *testing.T) {
	p := monitoredTestPool(t, map[string][]string{"shard": {"first:1", "second:1"}})
	var phase atomic.Int32
	var calls atomic.Int32
	m := NewPrimaryMonitor(20*time.Millisecond, time.Second, 2)
	defer m.Close()
	m.probe = func(_ context.Context, _ topology.TopologyMgr, _ *startup.StartupParams, _ *config.BackendRule, _ kr.ShardKey, host config.Host) (tsa.CheckResult, string, error) {
		calls.Add(1)
		switch phase.Load() {
		case 0:
			return tsa.CheckResult{Alive: true, RW: host.Address == "first:1", Reason: "initial"}, "", nil
		case 1:
			return tsa.CheckResult{Alive: true, RW: host.Address == "second:1", Reason: "changed"}, "", nil
		case 2:
			return tsa.CheckResult{}, "connect", errors.New("connection refused")
		default:
			return tsa.CheckResult{Alive: true, RW: true, Reason: "recovered"}, "", nil
		}
	}
	m.Register(p)
	awaitPrimary(t, p, "first:1", func(v tsa.CachedCheckResult) bool { return v.CR.RW })
	phase.Store(1)
	awaitPrimary(t, p, "second:1", func(v tsa.CachedCheckResult) bool { return v.CR.RW })
	require.False(t, p.InstanceHealthChecks()["first:1"].CR.RW)
	phase.Store(2)
	awaitPrimary(t, p, "first:1", func(v tsa.CachedCheckResult) bool { return !v.CR.Alive && strings.Contains(v.CR.Reason, "refused") })
	awaitPrimary(t, p, "second:1", func(v tsa.CachedCheckResult) bool { return !v.CR.Alive })
	phase.Store(3)
	require.Eventually(t, func() bool {
		for _, v := range p.InstanceHealthChecks() {
			if v.CR.Alive && v.CR.RW {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)
	require.Greater(t, calls.Load(), int32(4))
}

func TestPrimaryMonitorWorkerBudgetCancellationAndIndependentShards(t *testing.T) {
	p := monitoredTestPool(t, map[string][]string{"blocked": {"blocked:1"}, "healthy": {"healthy:1"}, "other": {"other:1"}})
	m := NewPrimaryMonitor(time.Hour, time.Second, 2)
	var active, maxActive atomic.Int32
	blocked := make(chan struct{})
	canceled := make(chan struct{})
	m.probe = func(ctx context.Context, _ topology.TopologyMgr, _ *startup.StartupParams, _ *config.BackendRule, _ kr.ShardKey, host config.Host) (tsa.CheckResult, string, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for old := maxActive.Load(); current > old; old = maxActive.Load() {
			if maxActive.CompareAndSwap(old, current) {
				break
			}
		}
		if host.Address == "blocked:1" {
			close(blocked)
			<-ctx.Done()
			close(canceled)
			return tsa.CheckResult{}, "query", ctx.Err()
		}
		return tsa.CheckResult{Alive: true, RW: true}, "", nil
	}
	m.Register(p)
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("blocked shard not scheduled")
	}
	awaitPrimary(t, p, "healthy:1", func(v tsa.CachedCheckResult) bool { return v.CR.RW })
	awaitPrimary(t, p, "other:1", func(v tsa.CachedCheckResult) bool { return v.CR.RW })
	require.LessOrEqual(t, maxActive.Load(), int32(2))
	m.Unregister(p)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("route removal did not cancel probe")
	}
	m.Close()
	require.Equal(t, int32(0), active.Load())
	require.Empty(t, p.InstanceHealthChecks())
	require.Empty(t, p.TsaCacheEntries())
}

func TestHostObservationOrderingGenerationAndScope(t *testing.T) {
	p := monitoredTestPool(t, map[string][]string{"one": {"host:1:sas"}, "two": {"host:1:sas"}})
	host := config.Host{Address: "host:1", AZ: "sas"}
	older := p.beginObservation("one", host)
	newer := p.beginObservation("one", host)
	require.True(t, p.publishObservation(newer, tsa.CheckResult{Reason: "failed authentication"}, "authentication", newer.started))
	require.False(t, p.publishObservation(older, tsa.CheckResult{Alive: true, RW: true}, "", older.started))
	require.False(t, p.InstanceHealthChecks()[host.Address].CR.Alive)
	other := p.beginObservation("two", host)
	require.True(t, p.publishObservation(other, tsa.CheckResult{Alive: true, RW: true}, "", other.started))
	first, _ := p.cache.matchShard("one", config.TargetSessionAttrsRW, host.Address, host.AZ)
	second, _ := p.cache.matchShard("two", config.TargetSessionAttrsRW, host.Address, host.AZ)
	require.False(t, first.Alive)
	require.True(t, second.Match)
	// Attempt ordering also applies when the clock returns the same timestamp.
	equalTime := p.beginObservation("two", host)
	require.True(t, p.publishObservation(equalTime, tsa.CheckResult{Reason: "new failure"}, "query", other.started))
	stored, ok := p.checker.CachedScoped("two", host.Address, host.AZ)
	require.True(t, ok)
	require.Equal(t, "new failure", stored.CR.Reason)
	stale := p.beginObservation("one", host)
	p.tmgr.DropShard("one")
	require.False(t, p.publishObservation(stale, tsa.CheckResult{Alive: true, RW: true}, "", stale.started))
	p.tmgr.AddShard(topology.DataShardFromConfig("one", &config.Shard{RawHosts: []string{"host:1:sas"}}))
	require.False(t, p.publishObservation(stale, tsa.CheckResult{Alive: true, RW: true}, "", stale.started))
	m := NewPrimaryMonitor(0, time.Second, 1)
	m.Register(p)
	final := p.beginObservation("two", host)
	m.Unregister(p)
	require.False(t, p.publishObservation(final, tsa.CheckResult{Alive: true, RW: true}, "", final.started))
	m.Close()
}

func TestPrimaryMonitorManualFailedScopeDisabledAndMetricsCleanup(t *testing.T) {
	p := monitoredTestPool(t, map[string][]string{"shard": {"failed:1", "unknown:1"}})
	p.cache.MarkUnmatched(config.TargetSessionAttrsRW, "failed:1", "", false, "old failure")
	m := NewPrimaryMonitor(0, time.Second, 1)
	defer m.Close()
	var count atomic.Int32
	m.probe = func(ctx context.Context, tm topology.TopologyMgr, sp *startup.StartupParams, r *config.BackendRule, k kr.ShardKey, h config.Host) (tsa.CheckResult, string, error) {
		count.Add(1)
		return writableProbe(ctx, tm, sp, r, k, h)
	}
	m.Register(p)
	registry := prometheus.NewRegistry()
	registry.MustRegister(m)
	require.Empty(t, p.InstanceHealthChecks())
	results := p.RunRecheckHosts()
	require.Len(t, results, 1)
	require.Equal(t, "failed:1", results[0].Host)
	require.Equal(t, tsa.TSA(config.TargetSessionAttrsRW), results[0].Tsa)
	require.True(t, results[0].Match)
	require.Equal(t, int32(1), count.Load())
	_, exists := p.InstanceHealthChecks()["unknown:1"]
	require.False(t, exists)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, families)
	m.Unregister(p)
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
	disabled := NewDBPoolWithDisabledFeatures(p.tmgr).(*DBPool)
	defer disabled.StopCacheWatchdog()
	disabled.SetRule(&config.BackendRule{Usr: "disabled", DB: "disabled"})
	m.Register(disabled)
	m.mu.Lock()
	_, registered := m.targets[disabled]
	m.mu.Unlock()
	require.False(t, registered)
}

func TestPrimaryAcquisitionErrorContainsHostAndReason(t *testing.T) {
	p := NewDBPoolWithAllocator(topology.TopMgrFromMap(topology.DataShardMapFromConfig(map[string]*config.Shard{"shard": {RawHosts: []string{"host:1"}}})), nil, func(kr.ShardKey, config.Host, *config.BackendRule) (shard.ShardHostInstance, error) {
		return nil, errors.New("password authentication failed")
	})
	defer p.StopCacheWatchdog()
	p.SetRule(&config.BackendRule{})
	_, err := p.ConnectionWithTSA(ConnAllocParams{Tsa: config.TargetSessionAttrsRW}, kr.ShardKey{Name: "shard"})
	require.ErrorContains(t, err, "host host:1: password authentication failed")
	require.Equal(t, "acquire", p.InstanceHealthChecks()["host:1"].FailureStage)
}
