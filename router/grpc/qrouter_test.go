package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/pg-sharding/spqr/pkg/client"
	mockClient "github.com/pg-sharding/spqr/pkg/mock/client"
	metaMock "github.com/pg-sharding/spqr/pkg/mock/meta"
	mockPool "github.com/pg-sharding/spqr/pkg/mock/pool"
	mockShard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/pool"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	mockRuleRouter "github.com/pg-sharding/spqr/router/mock/rulerouter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAddDataShardPassesRequestToManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := metaMock.NewMockEntityMgr(ctrl)
	mgr.EXPECT().AddDataShard(gomock.Any(), gomock.Any()).Times(1).Return(nil)

	server := &LocalQrouterServer{
		mgr: mgr,
	}

	_, err := server.AddDataShard(context.Background(), &protos.AddShardRequest{
		Shard: &protos.Shard{
			Id: "sh-bad",
			Options: []*protos.GenericOption{
				{Name: "host", Value: "127.0.0.1"},
			},
		},
	})

	assert.NoError(t, err)
}

func TestAddDataShardRejectsNilShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	server := &LocalQrouterServer{
		mgr: metaMock.NewMockEntityMgr(ctrl),
	}

	_, err := server.AddDataShard(context.Background(), &protos.AddShardRequest{})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func setupMockShardHostCtl(ctrl *gomock.Controller, id uint, host, keyName, usr, db string) shard.ShardHostCtl {
	m := mockShard.NewMockShardHostCtl(ctrl)
	m.EXPECT().ID().Return(id).AnyTimes()
	m.EXPECT().InstanceHostname().Return(host).AnyTimes()
	m.EXPECT().ShardKeyName().Return(keyName).AnyTimes()
	m.EXPECT().Usr().Return(usr).AnyTimes()
	m.EXPECT().DB().Return(db).AnyTimes()
	m.EXPECT().Sync().Return(int64(0)).AnyTimes()
	m.EXPECT().TxServed().Return(int64(0)).AnyTimes()
	m.EXPECT().TxStatus().Return(txstatus.TXIDLE).AnyTimes()
	return m
}

func TestListBackendConnections_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRR := mockRuleRouter.NewMockRuleRouter(ctrl)

	testShards := []shard.ShardHostCtl{
		setupMockShardHostCtl(ctrl, 1, "host1.local", "key1", "testuser1", "testdb1"),
		setupMockShardHostCtl(ctrl, 2, "host2.local", "key2", "testuser2", "testdb2"),
	}

	mockRR.EXPECT().
		ForEach(gomock.Any()).
		DoAndReturn(func(fn func(shard.ShardHostCtl) error) error {
			for _, sh := range testShards {
				if err := fn(sh); err != nil {
					return err
				}
			}
			return nil
		}).
		Times(1)

	server := &LocalQrouterServer{rr: mockRR}
	reply, err := server.ListBackendConnections(context.Background(), &emptypb.Empty{})

	assert.NoError(t, err)
	assert.Len(t, reply.Conns, 2)
	assert.Equal(t, uint64(1), reply.Conns[0].BackendConnectionId)
	assert.Equal(t, "host1.local", reply.Conns[0].Hostname)
	assert.Equal(t, "key1", reply.Conns[0].ShardKeyName)
}

func TestListBackendConnections_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRR := mockRuleRouter.NewMockRuleRouter(ctrl)

	mockRR.EXPECT().
		ForEach(gomock.Any()).
		DoAndReturn(func(fn func(shard.ShardHostCtl) error) error {
			return nil
		}).
		Times(1)

	server := &LocalQrouterServer{rr: mockRR}
	reply, err := server.ListBackendConnections(context.Background(), &emptypb.Empty{})

	assert.NoError(t, err)
	assert.Empty(t, reply.Conns)
}

func TestListBackendConnections_ForEachError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRR := mockRuleRouter.NewMockRuleRouter(ctrl)
	testErr := errors.New("router not initialized")

	mockRR.EXPECT().
		ForEach(gomock.Any()).
		Return(testErr).
		Times(1)

	server := &LocalQrouterServer{rr: mockRR}
	reply, err := server.ListBackendConnections(context.Background(), &emptypb.Empty{})

	assert.ErrorIs(t, err, testErr)
	assert.Empty(t, reply.Conns)
}

func setupMockClient(ctrl *gomock.Controller, id uint, usr, db string) *mockClient.MockClientInfo {
	m := mockClient.NewMockClientInfo(ctrl)
	m.EXPECT().ID().Return(id).AnyTimes()
	m.EXPECT().Usr().Return(usr).AnyTimes()
	m.EXPECT().DB().Return(db).AnyTimes()
	m.EXPECT().ShardingKey().Return("shard_key_1").AnyTimes()
	m.EXPECT().RAddr().Return("127.0.0.1:54321").AnyTimes()
	m.EXPECT().Shards().Return(nil).AnyTimes()
	return m
}

func TestListClients_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRR := mockRuleRouter.NewMockRuleRouter(ctrl)
	testClients := []client.ClientInfo{
		setupMockClient(ctrl, 1, "user1", "db1"),
		setupMockClient(ctrl, 2, "user2", "db2"),
	}

	mockRR.EXPECT().
		ClientPoolForeach(gomock.Any()).
		DoAndReturn(func(fn func(client.ClientInfo) error) error {
			for _, c := range testClients {
				if err := fn(c); err != nil {
					return err
				}
			}
			return nil
		}).
		Times(1)

	server := &LocalQrouterServer{rr: mockRR}
	reply, err := server.ListClients(context.Background(), &emptypb.Empty{})

	assert.NoError(t, err)
	assert.Len(t, reply.Clients, 2)
	assert.Equal(t, uint64(1), reply.Clients[0].ClientId)
	assert.Equal(t, "user1", reply.Clients[0].User)
	assert.Equal(t, "db1", reply.Clients[0].Dbname)
}

func TestListClients_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRR := mockRuleRouter.NewMockRuleRouter(ctrl)

	mockRR.EXPECT().
		ClientPoolForeach(gomock.Any()).
		DoAndReturn(func(fn func(client.ClientInfo) error) error {
			return nil
		}).
		Times(1)

	server := &LocalQrouterServer{rr: mockRR}
	reply, err := server.ListClients(context.Background(), &emptypb.Empty{})

	assert.NoError(t, err)
	assert.Empty(t, reply.Clients)
}

func TestListClients_ForEachError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRR := mockRuleRouter.NewMockRuleRouter(ctrl)
	testErr := errors.New("client pool down")

	mockRR.EXPECT().
		ClientPoolForeach(gomock.Any()).
		Return(testErr).
		Times(1)

	server := &LocalQrouterServer{rr: mockRR}
	reply, err := server.ListClients(context.Background(), &emptypb.Empty{})

	assert.ErrorIs(t, err, testErr)
	assert.Empty(t, reply.Clients)
}

func setupMockPool(ctrl *gomock.Controller) *mockPool.MockPool {
	m := mockPool.NewMockPool(ctrl)
	m.EXPECT().View().Return(pool.Statistics{}).AnyTimes()
	return m
}

func TestListPools_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRR := mockRuleRouter.NewMockRuleRouter(ctrl)
	testPools := []pool.Pool{setupMockPool(ctrl), setupMockPool(ctrl)}

	mockRR.EXPECT().
		ForEachPool(gomock.Any()).
		DoAndReturn(func(fn func(pool.Pool) error) error {
			for _, p := range testPools {
				if err := fn(p); err != nil {
					return err
				}
			}
			return nil
		}).
		Times(1)

	server := &LocalQrouterServer{rr: mockRR}
	reply, err := server.ListPools(context.Background(), &emptypb.Empty{})

	assert.NoError(t, err)
	assert.Len(t, reply.Pools, 2)
}

func TestListPools_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRR := mockRuleRouter.NewMockRuleRouter(ctrl)

	mockRR.EXPECT().
		ForEachPool(gomock.Any()).
		DoAndReturn(func(fn func(pool.Pool) error) error {
			return nil
		}).
		Times(1)

	server := &LocalQrouterServer{rr: mockRR}
	reply, err := server.ListPools(context.Background(), &emptypb.Empty{})

	assert.NoError(t, err)
	assert.Empty(t, reply.Pools)
}

func TestListPools_ForEachPoolError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRR := mockRuleRouter.NewMockRuleRouter(ctrl)
	testErr := errors.New("pool manager not initialized")

	mockRR.EXPECT().
		ForEachPool(gomock.Any()).
		Return(testErr).
		Times(1)

	server := &LocalQrouterServer{rr: mockRR}
	reply, err := server.ListPools(context.Background(), &emptypb.Empty{})

	assert.ErrorIs(t, err, testErr)
	assert.Empty(t, reply.Pools)
}
