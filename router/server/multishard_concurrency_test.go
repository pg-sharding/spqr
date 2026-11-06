package server_test

import (
	"context"
	"sync"
	"testing"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/engine"
	mockclient "github.com/pg-sharding/spqr/pkg/mock/client"
	mockconn "github.com/pg-sharding/spqr/pkg/mock/conn"
	mockpool "github.com/pg-sharding/spqr/pkg/mock/pool"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestMultiShardServer_ConcurrentClientScan(t *testing.T) {
	ctrl := gomock.NewController(t)
	provider := mockpool.NewMockConnectionProvider(ctrl)
	instance := mockconn.NewMockDBInstance(ctrl)
	instance.EXPECT().Hostname().Return("backend:5432").AnyTimes()

	shards := make(map[string]shard.ShardHostInstance)
	keys := []kr.ShardKey{{Name: "sh1"}, {Name: "sh2"}}
	for i, key := range keys {
		sh := mockshard.NewMockShardHostInstance(ctrl)
		sh.EXPECT().ID().Return(uint(i + 1)).AnyTimes()
		sh.EXPECT().Name().Return(key.Name).AnyTimes()
		sh.EXPECT().SHKey().Return(key).AnyTimes()
		sh.EXPECT().Instance().Return(instance).AnyTimes()
		sh.EXPECT().Cancel().Return(nil).AnyTimes()
		sh.EXPECT().CancellableIDs().Return([]uint32{uint32(i + 1)}).AnyTimes()
		shards[key.Name] = sh
	}
	provider.EXPECT().ConnectionWithTSA(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ pool.ConnAllocParams, key kr.ShardKey) (shard.ShardHostInstance, error) {
			return shards[key.Name], nil
		}).AnyTimes()

	srv, err := server.NewMultiShardServer(provider)
	require.NoError(t, err)
	for _, key := range keys {
		require.NoError(t, srv.AllocateGangMember(pool.ConnAllocParams{}, key))
	}

	cl := mockclient.NewMockClientInfo(ctrl)
	cl.EXPECT().Shards().DoAndReturn(srv.Datashards).AnyTimes()
	cl.EXPECT().ID().Return(uint(1)).AnyTimes()
	cl.EXPECT().CancelPID().Return(uint32(1)).AnyTimes()
	cl.EXPECT().Usr().Return("user").AnyTimes()
	cl.EXPECT().DB().Return("db").AnyTimes()
	cl.EXPECT().RAddr().Return("local").AnyTimes()
	cl.EXPECT().Conn().Return(nil).AnyTimes()
	cl.EXPECT().GetTimeQuantile(gomock.Any(), gomock.Any()).Return(float64(0)).AnyTimes()
	clients := []client.ClientInfo{cl}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for range 500 {
			for _, key := range keys {
				if _, err := srv.UnRouteShard(key); err != nil {
					t.Error(err)
					return
				}
				if err := srv.AllocateGangMember(pool.ConnAllocParams{}, key); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	close(start)
	for range 1000 {
		tts, err := engine.ClientsVirtualRelationScan(context.Background(), clients)
		if err != nil {
			t.Error(err)
			break
		}
		if len(tts.Raw) < 1 || len(tts.Raw) > 2 {
			t.Errorf("expected one or two active shards, got %d rows", len(tts.Raw))
		}
		for _, row := range tts.Raw {
			if got := string(row[4]); got != "backend:5432" {
				t.Errorf("unexpected backend hostname: %q", got)
			}
		}
		if err := srv.Cancel(); err != nil {
			t.Error(err)
			break
		}
		_ = srv.CancellableIDs()
	}
	wg.Wait()
}
