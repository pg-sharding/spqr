package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	mockconn "github.com/pg-sharding/spqr/pkg/mock/conn"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type cachedForegroundPool struct {
	ShardHostsPool
	connection shard.ShardHostInstance
}

func (p *cachedForegroundPool) ConnectionHost(uint, kr.ShardKey, config.Host) (shard.ShardHostInstance, error) {
	return p.connection, nil
}

func TestForegroundCachedChecksDoNotInvalidateFreshPrimaryProbe(t *testing.T) {
	for _, outcome := range []string{"failure", "demotion"} {
		t.Run(outcome, func(t *testing.T) {
			p := monitoredTestPool(t, map[string][]string{"shard": {"host:1"}})
			p.checker = tsa.NewCachedTSACheckerWithDuration(time.Hour)
			host := config.Host{Address: "host:1"}
			initial := p.beginObservation("shard", host)
			require.True(t, p.publishObservation(initial, tsa.CheckResult{Alive: true, RW: true, Reason: "old primary"}, "", initial.started))

			ctrl := gomock.NewController(t)
			instance := mockconn.NewMockDBInstance(ctrl)
			instance.EXPECT().Hostname().Return(host.Address).AnyTimes()
			instance.EXPECT().AvailabilityZone().Return("").AnyTimes()
			connection := mockshard.NewMockShardHostInstance(ctrl)
			connection.EXPECT().Instance().Return(instance).AnyTimes()
			p.pool = &cachedForegroundPool{ShardHostsPool: p.pool, connection: connection}

			m := NewPrimaryMonitor(time.Hour, time.Second, 1)
			defer m.Close()
			entered, release := make(chan struct{}), make(chan struct{})
			m.probe = func(ctx context.Context, _ topology.TopologyMgr, _ *startup.StartupParams, _ *config.BackendRule, _ kr.ShardKey, _ config.Host) (tsa.CheckResult, string, error) {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return tsa.CheckResult{}, "query", ctx.Err()
				}
				if outcome == "failure" {
					return tsa.CheckResult{}, "connect", errors.New("fresh connection refused")
				}
				return tsa.CheckResult{Alive: true, RW: false, Reason: "fresh replica"}, "", nil
			}
			m.Register(p)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("background probe did not start")
			}

			// These requests use the old, still-fresh TSA observation. Successful
			// acquisition and cache reads must not supersede a real SQL attempt.
			for range 20 {
				selected, err := p.ConnectionWithTSA(ConnAllocParams{Tsa: config.TargetSessionAttrsRW}, kr.ShardKey{Name: "shard"})
				require.NoError(t, err)
				require.Same(t, connection, selected)
			}
			close(release)
			awaitPrimary(t, p, host.Address, func(check tsa.CachedCheckResult) bool {
				return !check.CR.RW && (outcome != "failure" || !check.CR.Alive)
			})
			check := p.InstanceHealthChecks()[host.Address]
			require.True(t, check.LastCheckTime.After(initial.started))
			cached, ok := p.cache.matchShard("shard", config.TargetSessionAttrsRW, host.Address, "")
			require.True(t, ok)
			require.False(t, cached.Match)
			require.Equal(t, check.CR.Alive, cached.Alive)
		})
	}
}

func TestHostObservationRejectsTopologyABA(t *testing.T) {
	p := monitoredTestPool(t, map[string][]string{"shard": {"host:1"}})
	host := config.Host{Address: "host:1"}
	oldAttempt := p.beginObservation("shard", host)
	oldDefinition, err := p.tmgr.ShardById("shard")
	require.NoError(t, err)

	// No scheduler reconciliation or explicit observation cleanup happens in
	// between. Equal endpoint options still belong to a new topology generation.
	p.tmgr.DropShard("shard")
	p.tmgr.AddShard(topology.NewDataShard("shard", config.DataShard, oldDefinition.Options()))
	newDefinition, err := p.tmgr.ShardById("shard")
	require.NoError(t, err)
	require.NotSame(t, oldDefinition, newDefinition)
	require.Equal(t, oldDefinition.Options(), newDefinition.Options())
	require.False(t, p.publishObservation(oldAttempt, tsa.CheckResult{Alive: true, RW: true, Reason: "obsolete primary"}, "", oldAttempt.started))
	require.Empty(t, p.InstanceHealthChecks())
	require.Empty(t, p.TsaCacheEntries())

	currentAttempt := p.beginObservation("shard", host)
	require.True(t, p.publishObservation(currentAttempt, tsa.CheckResult{Reason: "current outage"}, "connect", currentAttempt.started))
	require.Equal(t, "current outage", p.InstanceHealthChecks()[host.Address].CR.Reason)
}

func TestHostObservationRejectsClearedShardGeneration(t *testing.T) {
	p := monitoredTestPool(t, map[string][]string{"shard": {"host:1"}})
	attempt := p.beginObservation("shard", config.Host{Address: "host:1"})
	p.clearShardObservations("shard")
	require.False(t, p.publishObservation(attempt, tsa.CheckResult{Alive: true, RW: true}, "", attempt.started))
	require.Empty(t, p.InstanceHealthChecks())
}

func TestManualRecheckDelayIncludesWaitingForRunningPass(t *testing.T) {
	p := monitoredTestPool(t, map[string][]string{"shard": {"host:1"}})
	p.cache.MarkUnmatched(config.TargetSessionAttrsRW, "host:1", "", false, "old failure")
	m := NewPrimaryMonitor(time.Hour, time.Second, 1)
	defer m.Close()
	registry := prometheus.NewRegistry()
	registry.MustRegister(m)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	m.probe = func(ctx context.Context, _ topology.TopologyMgr, _ *startup.StartupParams, _ *config.BackendRule, _ kr.ShardKey, _ config.Host) (tsa.CheckResult, string, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return tsa.CheckResult{}, "query", ctx.Err()
			}
		}
		return tsa.CheckResult{Alive: true, RW: true, Reason: "primary"}, "", nil
	}
	m.Register(p)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("initial discovery did not start")
	}
	manualResult := make(chan []HostRecheckResult, 1)
	go func() { manualResult <- p.RunRecheckHosts() }()
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return len(m.targets[p]["shard"].manual) == 1
	}, time.Second, time.Millisecond)
	// Keep the manual request queued behind the in-flight pass long enough to
	// distinguish submission delay from the later worker dispatch delay.
	time.Sleep(40 * time.Millisecond)
	close(release)
	select {
	case results := <-manualResult:
		require.Len(t, results, 1)
		require.True(t, results[0].Match)
	case <-time.After(time.Second):
		t.Fatal("manual recheck did not complete")
	}
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == "spqr_primary_probe_scheduling_delay_seconds" {
			require.Len(t, family.Metric, 1)
			require.Equal(t, uint64(2), family.Metric[0].Histogram.GetSampleCount())
			require.GreaterOrEqual(t, family.Metric[0].Histogram.GetSampleSum(), 0.04)
			return
		}
	}
	t.Fatal("missing scheduling delay metric")
}

func TestForegroundRechecksNegativeCacheAfterSuccessfulAcquisition(t *testing.T) {
	address, closed := startProbeBackend(t, false, func(backend *pgproto3.Backend, _ *pgproto3.StartupMessage) error {
		if err := probeBackendReady(backend); err != nil {
			return err
		}
		backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("off")}})
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SHOW")})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return backend.Flush()
	})
	p := monitoredTestPool(t, map[string][]string{"shard": {address}})
	p.checker = tsa.NewCachedTSACheckerWithDuration(time.Hour)
	token := p.beginObservation("shard", config.Host{Address: address})
	require.True(t, p.publishObservation(token, tsa.CheckResult{Reason: "previous connection refused"}, "connect", token.started))
	connection, err := p.ConnectionWithTSA(ConnAllocParams{Tsa: config.TargetSessionAttrsRW}, kr.ShardKey{Name: "shard"})
	require.NoError(t, err)
	require.True(t, p.InstanceHealthChecks()[address].CR.RW)
	require.NoError(t, p.Discard(connection))
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("recovered foreground connection leaked")
	}
}
