package pool

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type primaryPostgres struct {
	data, log, pgctl, address string
	running                   bool
}

func primaryPostgresCommand(t *testing.T, binary string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
	require.NoError(t, err, "%s %v: %s", binary, args, output)
}

// The integration test owns disposable clusters and never changes an existing
// PostgreSQL instance. PG_BINDIR can select a version installed outside PATH.
func newPrimaryPostgres(t *testing.T, readOnly bool) *primaryPostgres {
	t.Helper()
	if testing.Short() {
		t.Skip("local PostgreSQL integration is disabled by -short")
	}
	if os.Geteuid() == 0 {
		t.Skip("initdb cannot run as root")
	}
	find := func(name string) string {
		if directory := os.Getenv("PG_BINDIR"); directory != "" {
			name = filepath.Join(directory, name)
		}
		binary, err := exec.LookPath(name)
		if err != nil {
			t.Skipf("local PostgreSQL integration requires %s: %v", name, err)
		}
		return binary
	}
	initdb, pgctl := find("initdb"), find("pg_ctl")
	directory := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	pg := &primaryPostgres{data: filepath.Join(directory, "data"), log: filepath.Join(directory, "postgres.log"), pgctl: pgctl, address: address}
	primaryPostgresCommand(t, initdb, "-D", pg.data, "-U", "postgres", "-A", "trust", "--no-locale", "--no-sync", "-E", "UTF8")
	_, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	settings := fmt.Sprintf("\nlisten_addresses = '127.0.0.1'\nport = %s\nunix_socket_directories = ''\nfsync = off\nsynchronous_commit = off\nfull_page_writes = off\nmax_connections = 20\n", port)
	file, err := os.OpenFile(filepath.Join(pg.data, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = file.WriteString(settings)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	pg.writeReadOnly(t, readOnly)
	t.Cleanup(func() {
		if pg.running {
			pg.stop(t)
		}
	})
	pg.start(t)
	return pg
}

func (pg *primaryPostgres) writeReadOnly(t *testing.T, readOnly bool) {
	t.Helper()
	value := "off"
	if readOnly {
		value = "on"
	}
	require.NoError(t, os.WriteFile(filepath.Join(pg.data, "postgresql.auto.conf"), []byte("default_transaction_read_only = '"+value+"'\n"), 0600))
}

func (pg *primaryPostgres) setReadOnly(t *testing.T, readOnly bool) {
	t.Helper()
	pg.writeReadOnly(t, readOnly)
	primaryPostgresCommand(t, pg.pgctl, "-D", pg.data, "reload")
	want := "off"
	if readOnly {
		want = "on"
	}
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		connection, err := pgx.Connect(ctx, "postgres://postgres@"+pg.address+"/postgres?sslmode=disable")
		if err != nil {
			return false
		}
		defer func() { _ = connection.Close(ctx) }()
		var actual string
		return connection.QueryRow(ctx, "SHOW transaction_read_only").Scan(&actual) == nil && actual == want
	}, 5*time.Second, 10*time.Millisecond, "PostgreSQL did not reload transaction_read_only=%s", want)
}

func (pg *primaryPostgres) start(t *testing.T) {
	t.Helper()
	primaryPostgresCommand(t, pg.pgctl, "-D", pg.data, "-l", pg.log, "-w", "start")
	pg.running = true
}

func (pg *primaryPostgres) stop(t *testing.T) {
	t.Helper()
	primaryPostgresCommand(t, pg.pgctl, "-D", pg.data, "-m", "fast", "-w", "stop")
	pg.running = false
}

func primaryIntegrationPool(mapping map[string]*topology.DataShard, user string) *DBPool {
	p := NewDBPool(topology.TopMgrFromMap(mapping), &startup.StartupParams{}, "", time.Hour, 0).(*DBPool)
	p.StopBackgroundHealthCheck()
	p.ShuffleHosts = false
	p.SetRule(&config.BackendRule{Usr: user, DB: "postgres", DefaultAuthRule: &config.AuthBackendCfg{Usr: user}, ConnectionLimit: 1, ConnectionRetries: 1, ConnectionTimeout: time.Second})
	return p
}

func primaryIntegrationShard(name string, hosts ...string) *topology.DataShard {
	return topology.DataShardFromConfig(name, &config.Shard{Type: config.DataShard, RawHosts: hosts})
}

func requirePrimaryState(t *testing.T, p *DBPool, host string, alive, writable bool, after time.Time) {
	t.Helper()
	require.Eventually(t, func() bool {
		result, ok := p.InstanceHealthChecks()[host]
		if !ok || !result.LastCheckTime.After(after) || result.CR.Alive != alive || result.CR.RW != writable {
			return false
		}
		entry, ok := p.TsaCacheEntries()[TsaKey{Shard: "sh1", Host: host, Tsa: config.TargetSessionAttrsRW}]
		return ok && entry.LastCheckTime.Equal(result.LastCheckTime) && entry.Result.Alive == alive && entry.Result.Match == (alive && writable)
	}, 5*time.Second, 5*time.Millisecond, "host %s did not reach alive=%t writable=%t; snapshot=%+v", host, alive, writable, p.InstanceHealthChecks())
}

// PostgreSQL's default_transaction_read_only setting simulates demotion and
// promotion according to SPQR's writable-candidate definition. The servers are
// independent disposable clusters, not a physical-replication failover setup.
func TestPrimaryMonitorPostgresIdleFailover(t *testing.T) {
	primary := newPrimaryPostgres(t, false)
	replacement := newPrimaryPostgres(t, true)
	p := primaryIntegrationPool(map[string]*topology.DataShard{"sh1": primaryIntegrationShard("sh1", primary.address, replacement.address)}, "postgres")
	t.Cleanup(p.StopCacheWatchdog)
	require.Empty(t, p.InstanceHealthChecks())
	require.Empty(t, p.TsaCacheEntries())
	m := NewPrimaryMonitor(30*time.Millisecond, time.Second, 2)
	t.Cleanup(m.Close)
	m.Register(p)
	requirePrimaryState(t, p, primary.address, true, true, time.Time{})
	firstCheck := p.InstanceHealthChecks()[primary.address].LastCheckTime
	requirePrimaryState(t, p, primary.address, true, true, firstCheck)

	// Occupy the only application slot on both candidates. No application
	// queries or acquisitions occur during the subsequent failover and recovery.
	for index, host := range []string{primary.address, replacement.address} {
		connection, err := p.ConnectionHost(uint(index+1), kr.ShardKey{Name: "sh1"}, config.Host{Address: host})
		require.NoError(t, err)
		t.Cleanup(func() { _ = p.Discard(connection) })
	}
	var occupied int
	require.NoError(t, p.ForEachPool(func(applicationPool Pool) error {
		view := applicationPool.View()
		require.EqualValues(t, 1, view.UsedConnections)
		require.Zero(t, view.QueueResidualSize)
		occupied++
		return nil
	}))
	require.Equal(t, 2, occupied)

	demotion := time.Now()
	primary.setReadOnly(t, true)
	replacement.setReadOnly(t, false)
	requirePrimaryState(t, p, primary.address, true, false, demotion)
	requirePrimaryState(t, p, replacement.address, true, true, demotion)

	primary.stop(t)
	replacement.stop(t)
	outage := time.Now()
	requirePrimaryState(t, p, primary.address, false, false, outage)
	requirePrimaryState(t, p, replacement.address, false, false, outage)
	for _, host := range []string{primary.address, replacement.address} {
		result := p.InstanceHealthChecks()[host]
		require.NotEmpty(t, result.CR.Reason)
		require.Equal(t, "connect", result.FailureStage)
		require.False(t, result.LastSuccessTime.IsZero())
	}

	recovery := time.Now()
	replacement.start(t)
	requirePrimaryState(t, p, replacement.address, true, true, recovery)
	require.False(t, p.InstanceHealthChecks()[primary.address].CR.Alive)

	m.Close()
	require.Empty(t, p.InstanceHealthChecks())
	require.Empty(t, p.TsaCacheEntries())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	inspection, err := pgx.Connect(ctx, "postgres://postgres@"+replacement.address+"/postgres?sslmode=disable")
	require.NoError(t, err)
	defer func() { _ = inspection.Close(ctx) }()
	require.Eventually(t, func() bool {
		var connections int
		return inspection.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'app'").Scan(&connections) == nil && connections == 0
	}, time.Second, 10*time.Millisecond, "probe connections remained open after monitor shutdown")
}

// This responder exercises real loopback TCP, startup authentication, and the
// TSA query. It deliberately excludes PostgreSQL process, TLS, and password
// authentication costs so the benchmark isolates router scheduling overhead.
func primaryBenchmarkBackend(b *testing.B) string {
	b.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(b, err)
	var workers sync.WaitGroup
	var connections sync.Map
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Store(connection, true)
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer connections.Delete(connection)
				defer func() { _ = connection.Close() }()
				_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
				backend := pgproto3.NewBackend(connection, connection)
				if _, err := backend.ReceiveStartupMessage(); err != nil {
					return
				}
				backend.Send(&pgproto3.AuthenticationOk{})
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if backend.Flush() != nil {
					return
				}
				message, err := backend.Receive()
				if err != nil {
					return
				}
				query, ok := message.(*pgproto3.Query)
				if !ok || !strings.EqualFold(query.String, "SHOW transaction_read_only") {
					return
				}
				backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("transaction_read_only"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}}})
				backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("off")}})
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SHOW")})
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				_ = backend.Flush()
			}()
		}
	}()
	b.Cleanup(func() {
		_ = listener.Close()
		<-stopped
		connections.Range(func(key, _ any) bool {
			_ = key.(net.Conn).Close()
			return true
		})
		workers.Wait()
	})
	return listener.Addr().String()
}

func BenchmarkPrimaryMonitorFreshConnections(b *testing.B) {
	previousLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(previousLogLevel)
	for _, size := range []struct{ routes, shards int }{{1, 16}, {10, 16}, {100, 32}} {
		b.Run(fmt.Sprintf("routes=%d/shards=%d", size.routes, size.shards), func(b *testing.B) {
			b.StopTimer()
			address := primaryBenchmarkBackend(b)
			mapping := make(map[string]*topology.DataShard, size.shards)
			for shard := range size.shards {
				name := fmt.Sprintf("shard-%d", shard)
				mapping[name] = primaryIntegrationShard(name, address)
			}
			targetCount := size.routes * size.shards
			var totalProbes, maximum, totalAllocated uint64
			var duration time.Duration
			for range b.N {
				// A long interval measures exactly one initial discovery sweep.
				m := NewPrimaryMonitor(time.Hour, time.Second, 4)
				var active, peak, probes atomic.Int64
				m.probe = func(ctx context.Context, tmgr topology.TopologyMgr, sp *startup.StartupParams, rule *config.BackendRule, key kr.ShardKey, host config.Host) (result tsa.CheckResult, stage string, err error) {
					current := active.Add(1)
					for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
					}
					defer active.Add(-1)
					defer probes.Add(1)
					return probePrimary(ctx, tmgr, sp, rule, key, host)
				}
				pools := make([]*DBPool, size.routes)
				for route := range size.routes {
					pools[route] = primaryIntegrationPool(mapping, fmt.Sprintf("route-%d", route))
				}
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				started := time.Now()
				b.StartTimer()
				for _, p := range pools {
					m.Register(p)
				}
				deadline := time.Now().Add(30 * time.Second)
				for {
					ready := 0
					m.mu.Lock()
					for _, targets := range m.targets {
						for _, target := range targets {
							if target.preferred.Address != "" && !target.running {
								ready++
							}
						}
					}
					m.mu.Unlock()
					if ready == targetCount {
						break
					}
					if time.Now().After(deadline) {
						m.Close()
						b.Fatalf("discovery completed %d of %d targets", ready, targetCount)
					}
					time.Sleep(time.Millisecond)
				}
				b.StopTimer()
				duration += time.Since(started)
				runtime.ReadMemStats(&after)
				totalAllocated += after.TotalAlloc - before.TotalAlloc
				totalProbes += uint64(probes.Load())
				maximum = max(maximum, uint64(peak.Load()))
				m.Close()
				for _, p := range pools {
					p.StopCacheWatchdog()
				}
			}
			b.ReportAllocs()
			b.ReportMetric(float64(totalProbes)/duration.Seconds(), "connections/s")
			b.ReportMetric(float64(totalProbes)/float64(b.N), "connections/sweep")
			b.ReportMetric(float64(totalAllocated)/float64(targetCount*b.N), "allocated-B/target")
			b.ReportMetric(float64(maximum), "peak-probes")
			b.ReportMetric(5, "monitor-goroutines")
		})
	}
}
