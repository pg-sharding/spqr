package rulerouter

import (
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func monitoringConfig() *config.Router {
	cfg := &config.Router{}
	cfg.ApplyDefaults()
	cfg.DbpoolPrimaryCheckInterval = time.Hour
	return cfg
}

func configuredRoutes(t *testing.T, r *RuleRouterImpl) map[route.Key]*route.Route {
	t.Helper()
	routes := map[route.Key]*route.Route{}
	require.NoError(t, r.NotifyRoutes(func(rt *route.Route) (bool, error) {
		routes[*route.NewRouteKey(rt.BeRule().Usr, rt.BeRule().DB)] = rt
		return true, nil
	}))
	return routes
}

func TestPrimaryMonitoringInitializesMatchingConcreteRoutes(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			cfg := monitoringConfig()
			if !enabled {
				cfg.DbpoolPrimaryCheckInterval = 0
			}
			cfg.FrontendRules = []*config.FrontendRule{
				{Usr: "explicit", DB: "db"}, {Usr: "frontend", DB: "db"},
				{PoolDefault: true}, {Usr: "admin", DB: "spqr-console"},
			}
			cfg.BackendRules = []*config.BackendRule{
				{Usr: "explicit", DB: "db"}, {Usr: "backend", DB: "db"}, {PoolDefault: true},
			}
			r := NewRouter(topology.TopMgrFromMap(nil), nil, cfg, nil)
			t.Cleanup(func() { require.NoError(t, r.Shutdown()) })
			routes := configuredRoutes(t, r)
			if !enabled {
				require.Empty(t, routes)
				return
			}
			require.Len(t, routes, 3)
			for _, user := range []string{"explicit", "frontend", "backend"} {
				rt := routes[*route.NewRouteKey(user, "db")]
				require.NotNil(t, rt)
				require.Equal(t, user, rt.BeRule().Usr)
				require.Equal(t, user, rt.FrRule().Usr)
			}
		})
	}
}

func TestPrimaryMonitoringSkipsUnmatchedRoutes(t *testing.T) {
	cfg := monitoringConfig()
	cfg.FrontendRules = []*config.FrontendRule{{Usr: "frontend", DB: "db"}}
	cfg.BackendRules = []*config.BackendRule{{Usr: "backend", DB: "db"}}
	r := NewRouter(topology.TopMgrFromMap(nil), nil, cfg, nil)
	t.Cleanup(func() { require.NoError(t, r.Shutdown()) })
	require.Empty(t, configuredRoutes(t, r))
}

func TestRouteRegistrationAndShutdown(t *testing.T) {
	cfg := monitoringConfig()
	cfg.FrontendRules = []*config.FrontendRule{{PoolDefault: true}}
	cfg.BackendRules = []*config.BackendRule{{PoolDefault: true}}
	r := NewRouter(topology.TopMgrFromMap(nil), nil, cfg, nil)
	t.Cleanup(func() { require.NoError(t, r.Shutdown()) })
	key := *route.NewRouteKey("dynamic-user", "dynamic-db")
	var workers sync.WaitGroup
	results := make(chan *route.Route, 20)
	for range 20 {
		workers.Go(func() {
			rt, _, err := r.resolveRoute(key)
			if err != nil {
				t.Error(err)
				return
			}
			results <- rt
		})
	}
	workers.Wait()
	close(results)
	var winner *route.Route
	for rt := range results {
		if winner == nil {
			winner = rt
		}
		require.Same(t, winner, rt)
	}
	require.Len(t, configuredRoutes(t, r), 1)
	r.Obsolete(key)
	require.Empty(t, configuredRoutes(t, r))
	require.NoError(t, r.Shutdown())
	_, _, err := r.resolveRoute(key)
	require.ErrorContains(t, err, "shutting down")
}

// testPrimaryBackend speaks the actual startup and simple-query protocol so the
// lifecycle tests exercise discovery without application connections.
func testPrimaryBackend(t *testing.T, handler func(*pgproto3.Backend, *pgproto3.StartupMessage)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var active sync.Map
	var workers sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			active.Store(connection, struct{}{})
			workers.Go(func() {
				defer active.Delete(connection)
				defer func() { _ = connection.Close() }()
				backend := pgproto3.NewBackend(connection, connection)
				message, err := backend.ReceiveStartupMessage()
				if err != nil {
					return
				}
				startup, ok := message.(*pgproto3.StartupMessage)
				if !ok {
					t.Errorf("unexpected startup message: %T", message)
					return
				}
				handler(backend, startup)
			})
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-acceptDone
		active.Range(func(key, _ any) bool { _ = key.(net.Conn).Close(); return true })
		workers.Wait()
	})
	return listener.Addr().String()
}

func primaryReply(backend *pgproto3.Backend, readOnly bool) {
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if backend.Flush() != nil {
		return
	}
	if _, err := backend.Receive(); err != nil {
		return
	}
	value := "off"
	if readOnly {
		value = "on"
	}
	backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(value)}})
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SHOW")})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	_ = backend.Flush()
}

func primaryTopology(address string) topology.TopologyMgr {
	return topology.TopMgrFromMap(map[string]*topology.DataShard{
		"shard": topology.NewDataShard("shard", config.DataShard, topology.HostsToOptions([]string{address})),
	})
}

func TestDynamicRouteDiscoversAfterBackendRuleInstallation(t *testing.T) {
	startup := make(chan *pgproto3.StartupMessage, 1)
	address := testPrimaryBackend(t, func(backend *pgproto3.Backend, message *pgproto3.StartupMessage) {
		startup <- message
		primaryReply(backend, false)
	})
	cfg := monitoringConfig()
	cfg.FrontendRules = []*config.FrontendRule{{PoolDefault: true, SearchPath: "tenant_schema"}}
	cfg.BackendRules = []*config.BackendRule{{PoolDefault: true, DefaultAuthRule: &config.AuthBackendCfg{Usr: "backend-user"}}}
	r := NewRouter(primaryTopology(address), nil, cfg, nil)
	t.Cleanup(func() { require.NoError(t, r.Shutdown()) })
	require.Empty(t, configuredRoutes(t, r))
	rt, _, err := r.resolveRoute(*route.NewRouteKey("frontend-user", "tenant-db"))
	require.NoError(t, err)
	require.Equal(t, "frontend-user", rt.BeRule().Usr)
	select {
	case message := <-startup:
		require.Equal(t, "backend-user", message.Parameters["user"])
		require.Equal(t, "tenant-db", message.Parameters["database"])
		require.Equal(t, "tenant_schema", message.Parameters["search_path"])
	case <-time.After(3 * time.Second):
		t.Fatal("dynamic route was not probed")
	}
	require.Eventually(t, func() bool {
		for _, check := range rt.MultiShardPool().InstanceHealthChecks() {
			return check.CR.Alive && check.CR.RW
		}
		return false
	}, 3*time.Second, time.Millisecond)
}

func TestInstanceHealthChecksKeepsNewestRouteObservation(t *testing.T) {
	address := testPrimaryBackend(t, func(backend *pgproto3.Backend, message *pgproto3.StartupMessage) {
		primaryReply(backend, message.Parameters["user"] == "newer")
	})
	cfg := monitoringConfig()
	cfg.FrontendRules = []*config.FrontendRule{{PoolDefault: true}}
	cfg.BackendRules = []*config.BackendRule{{PoolDefault: true}}
	r := NewRouter(primaryTopology(address), nil, cfg, nil)
	t.Cleanup(func() { require.NoError(t, r.Shutdown()) })
	older, _, err := r.resolveRoute(*route.NewRouteKey("older", "db"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(older.MultiShardPool().InstanceHealthChecks()) == 1 }, 3*time.Second, time.Millisecond)
	newer, _, err := r.resolveRoute(*route.NewRouteKey("newer", "db"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(newer.MultiShardPool().InstanceHealthChecks()) == 1 }, 3*time.Second, time.Millisecond)
	health := r.InstanceHealthChecks()
	require.Len(t, health, 1)
	for _, check := range health {
		require.True(t, check.CR.Alive)
		require.False(t, check.CR.RW)
	}
}

func TestRouteRemovalAndShutdownCancelProbe(t *testing.T) {
	for _, remove := range []bool{true, false} {
		t.Run(map[bool]string{true: "remove", false: "shutdown"}[remove], func(t *testing.T) {
			started, closed := make(chan struct{}), make(chan struct{})
			address := testPrimaryBackend(t, func(backend *pgproto3.Backend, _ *pgproto3.StartupMessage) {
				close(started)
				_, _ = backend.Receive()
				close(closed)
			})
			cfg := monitoringConfig()
			cfg.DbpoolPrimaryCheckTimeout = time.Hour
			cfg.FrontendRules = []*config.FrontendRule{{Usr: "user", DB: "db"}}
			cfg.BackendRules = []*config.BackendRule{{Usr: "user", DB: "db"}}
			r := NewRouter(primaryTopology(address), nil, cfg, nil)
			t.Cleanup(func() { require.NoError(t, r.Shutdown()) })
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("startup discovery did not run")
			}
			if remove {
				r.Obsolete(*route.NewRouteKey("user", "db"))
			} else {
				require.NoError(t, r.Shutdown())
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("cancellation did not close the probe connection")
			}
			require.Empty(t, r.InstanceHealthChecks())
		})
	}
}

func TestReloadReplacesMonitoringRoutesAndCollector(t *testing.T) {
	previousCfg := *config.RouterConfig()
	previousTopology := topology.TopMgr
	t.Cleanup(func() { *config.RouterConfig() = previousCfg; topology.TopMgr = previousTopology })
	cfg := monitoringConfig()
	cfg.FrontendRules = []*config.FrontendRule{{Usr: "old", DB: "db"}}
	cfg.BackendRules = []*config.BackendRule{{Usr: "old", DB: "db"}}
	r := NewRouter(topology.TopMgrFromMap(nil), nil, cfg, nil)
	t.Cleanup(func() { require.NoError(t, r.Shutdown()) })
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(r.PrimaryMonitorCollector()))
	oldMonitor := r.routePool.monitor
	path := filepath.Join(t.TempDir(), "router.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`dbpool_primary_check_interval: 1h
frontend_rules:
  - usr: new
    db: db
    auth_rule:
      auth_method: ok
backend_rules:
  - usr: new
    db: db
`), 0600))
	require.NoError(t, r.Reload(path))
	require.NotSame(t, oldMonitor, r.routePool.monitor)
	routes := configuredRoutes(t, r)
	require.Len(t, routes, 1)
	require.Contains(t, routes, *route.NewRouteKey("new", "db"))
	_, err := registry.Gather()
	require.NoError(t, err)
}
