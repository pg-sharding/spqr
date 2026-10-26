//go:build adminconsolestress

package adminconsole_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/datashard"
	"github.com/pg-sharding/spqr/pkg/engine"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	routerclient "github.com/pg-sharding/spqr/router/client"
	routergrpc "github.com/pg-sharding/spqr/router/grpc"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/pg-sharding/spqr/router/statistics"
)

// Only the transport is replaced: connection state, digests, prepared-statement
// maps, routing and admin/gRPC scans are the production implementations.
type memoryBackend struct{ conn.DBInstance }

func (memoryBackend) Hostname() string                    { return "memory:5432" }
func (memoryBackend) Status() conn.InstanceStatus         { return conn.ACQUIRED }
func (memoryBackend) Send(pgproto3.FrontendMessage) error { return nil }
func (memoryBackend) Receive() (pgproto3.BackendMessage, error) {
	return &pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}, nil
}

func newBackend(t *testing.T, name string) shard.ShardHostInstance {
	t.Helper()
	sh, err := datashard.NewShardHostInstance(kr.ShardKey{Name: name}, memoryBackend{},
		&topology.DataShard{}, &config.BackendRule{Usr: "stress", DB: "stress"}, &startup.StartupParams{})
	if err != nil {
		t.Fatal(err)
	}
	sh.SetTxStatus(txstatus.TXIDLE)
	return sh
}

func newClient() routerclient.RouterClient {
	return routerclient.NewPsqlClient(nil, port.DefaultRouterPortType, false, "")
}

func seeds(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{0x80, 0x81, 0x82, 0x83, 0xff})
}

// A single owner goroutine performs legal session operations while an observer
// scans it. Bytes choose operations and yield points; fuzzing also varies the
// scheduling between actors. Every invocation joins both actors before cleanup.
func interleave(t *testing.T, input []byte, owner, observer func(byte) error) {
	t.Helper()
	if len(input) == 0 {
		input = []byte{0}
	}
	if len(input) > 64 {
		input = input[:64]
	}
	iterations := 128 + 4*len(input)
	start := make(chan struct{})
	errors := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := range iterations {
			b := input[i%len(input)]
			if b&0x80 != 0 {
				runtime.Gosched()
			}
			if err := owner(b); err != nil {
				errors <- err
				return
			}
		}
	}()
	close(start)
	var readErr error
	for i := range iterations {
		b := input[(i+1)%len(input)]
		if b&0x80 != 0 {
			runtime.Gosched()
		}
		if readErr = observer(b); readErr != nil {
			break
		}
	}
	wg.Wait()
	if readErr != nil {
		t.Fatal(readErr)
	}
	select {
	case err := <-errors:
		t.Fatal(err)
	default:
	}
}

func FuzzAdminBackendConnections(f *testing.F) {
	seeds(f)
	f.Fuzz(func(t *testing.T, input []byte) {
		sh := newBackend(t, "sh1")
		interleave(t, input, func(b byte) error {
			switch b % 4 {
			case 0, 1:
				var msg pgproto3.FrontendMessage = &pgproto3.Query{String: "SELECT 1"}
				if b%4 == 1 {
					msg = &pgproto3.Sync{}
				}
				if err := sh.Send(msg); err != nil {
					return err
				}
				_, err := sh.Receive()
				return err
			case 2:
				sh.SetTxStatus(txstatus.TXACT)
			case 3:
				sh.MarkStale()
			}
			return nil
		}, func(b byte) error {
			if b&1 != 0 {
				_ = routergrpc.ShardToProto(sh)
				return nil
			}
			tts, err := engine.BackendConnectionsVirtualRelationScan([]shard.ShardHostCtl{sh})
			if err == nil && (len(tts.Raw) != 1 || len(tts.Raw[0]) != len(tts.Desc)) {
				return fmt.Errorf("inconsistent backend row")
			}
			return err
		})
	})
}

func FuzzAdminClientQuantiles(f *testing.F) {
	seeds(f)
	f.Fuzz(func(t *testing.T, input []byte) {
		statistics.InitStatistics([]float64{0.5, 0.9, 0.99})
		t.Cleanup(func() { statistics.InitStatistics(nil) })
		cl := newClient()
		info := client.ClientInfoImpl{Client: cl}
		for i := range 64 {
			if err := cl.Add(statistics.StatisticsTypeRouter, float64(i+1)); err != nil {
				t.Fatal(err)
			}
			if err := cl.Add(statistics.StatisticsTypeShard, float64(i+1)); err != nil {
				t.Fatal(err)
			}
		}
		interleave(t, input, func(b byte) error {
			finish := time.Now()
			start := finish.Add(-time.Duration(b+1) * time.Millisecond)
			statistics.RecordStartTime(statistics.StatisticsTypeRouter, start, cl)
			statistics.RecordStartTime(statistics.StatisticsTypeShard, start, cl)
			statistics.RecordFinishedTransaction(finish, cl)
			return nil
		}, func(byte) error {
			tts, err := engine.ClientsVirtualRelationScan(context.Background(), []client.ClientInfo{info})
			if err == nil && (len(tts.Raw) != 1 || len(tts.Raw[0]) != len(tts.Desc)) {
				return fmt.Errorf("inconsistent client row")
			}
			return err
		})
	})
}

func FuzzAdminPreparedStatements(f *testing.F) {
	seeds(f)
	f.Fuzz(func(t *testing.T, input []byte) {
		sh := newBackend(t, "sh1")
		store := func(b byte) error {
			hash := uint64(b % 16)
			return sh.StorePrepareStatement(hash, sh.ID(), &prepstatement.PreparedStatementDefinition{
				Name: fmt.Sprintf("stmt_%d", hash), Query: "SELECT 1",
			}, &prepstatement.PreparedStatementDescriptor{})
		}
		for b := range byte(16) {
			if err := store(b); err != nil {
				t.Fatal(err)
			}
		}
		interleave(t, input, store, func(byte) error {
			rows := sh.ListPreparedStatements()
			_ = engine.PreparedStatementsVirtualRelationScan(context.Background(), rows)
			return nil
		})
	})
}

type memoryPool struct {
	pool.ConnectionProvider
	shards map[string]shard.ShardHostInstance
}

func (p memoryPool) ConnectionWithTSA(_ pool.ConnAllocParams, key kr.ShardKey) (shard.ShardHostInstance, error) {
	return p.shards[key.Name], nil
}

func FuzzAdminClientRouting(f *testing.F) {
	seeds(f)
	f.Fuzz(func(t *testing.T, input []byte) {
		statistics.InitStatistics(nil)
		shards := map[string]shard.ShardHostInstance{"sh1": newBackend(t, "sh1"), "sh2": newBackend(t, "sh2")}
		srv, err := server.NewMultiShardServer(memoryPool{shards: shards})
		if err != nil {
			t.Fatal(err)
		}
		cl := newClient()
		info := client.ClientInfoImpl{Client: cl}
		present := map[string]bool{}
		interleave(t, input, func(b byte) error {
			key := kr.ShardKey{Name: "sh1"}
			if b&1 != 0 {
				key.Name = "sh2"
			}
			switch b % 4 {
			case 0, 1:
				if present[key.Name] {
					_, err := srv.UnRouteShard(key)
					present[key.Name] = false
					return err
				}
				present[key.Name] = true
				return srv.AllocateGangMember(pool.ConnAllocParams{}, key)
			case 2:
				return cl.SwitchServerConn(srv)
			case 3:
				return cl.Unroute()
			}
			return nil
		}, func(b byte) error {
			_ = cl.CancellableIDs()
			if b&1 != 0 {
				_ = routergrpc.ClientToProto(info)
				return nil
			}
			_, err := engine.ClientsVirtualRelationScan(context.Background(), []client.ClientInfo{info})
			return err
		})
	})
}
