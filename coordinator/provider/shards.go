package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ShardServer struct {
	protos.UnimplementedShardServiceServer

	impl coordinator.Coordinator
}

func NewShardServer(impl coordinator.Coordinator) *ShardServer {
	return &ShardServer{
		impl: impl,
	}
}

var _ protos.ShardServiceServer = &ShardServer{}

// TODO : unit tests
func (s *ShardServer) AddDataShard(ctx context.Context, request *protos.AddShardRequest) (*emptypb.Empty, error) {
	newShard := request.GetShard()

	if err := s.impl.AddDataShard(ctx, topology.DataShardFromProto(newShard)); err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *ShardServer) AddWorldShard(ctx context.Context, request *protos.AddWorldShardRequest) (*emptypb.Empty, error) {
	panic("ShardServer.AddWorldShard not implemented")
}

// TODO: remove ShardRequest.
func (s *ShardServer) ListShards(ctx context.Context, _ *emptypb.Empty) (*protos.ListShardsReply, error) {
	shardList, err := s.impl.ListShards(ctx)
	if err != nil {
		return nil, err
	}

	protoShards := make([]*protos.Shard, 0, len(shardList))

	for _, sh := range shardList {
		protoShards = append(protoShards, topology.DataShardToProto(sh))
	}

	return &protos.ListShardsReply{
		Shards: protoShards,
	}, nil
}

// TODO : unit tests
func (s *ShardServer) GetShard(ctx context.Context, shardRequest *protos.ShardRequest) (*protos.ShardReply, error) {
	sh, err := s.impl.GetShard(ctx, shardRequest.Id)
	if err != nil {
		return nil, err
	}

	return &protos.ShardReply{
		Shard: topology.DataShardToProto(sh),
	}, nil
}

type CoordShardInfo struct {
	underlying *protos.BackendConnectionsInfo
	router     string
}

// MarkStale implements shard.ShardHostCtl.
func (c *CoordShardInfo) MarkStale() {
	/* noop */
}
func (c *CoordShardInfo) IsStale() bool {
	return false
}

// DataPending implements shard.Shardinfo.
func (c *CoordShardInfo) DataPending() bool {
	panic("CoordShardInfo.DataPending not implemented")
}

// Pid implements shard.Shardinfo.
func (c *CoordShardInfo) Pid() uint32 {
	return 0
}

// ListPreparedStatements implements shard.Shardinfo.
func (c *CoordShardInfo) ListPreparedStatements() []shard.PreparedStatementsMgrDescriptor {
	return nil
}

func NewCoordShardInfo(conn *protos.BackendConnectionsInfo, router string) shard.ShardHostCtl {
	return &CoordShardInfo{
		underlying: conn,
		router:     router,
	}
}

func (c *CoordShardInfo) DB() string {
	return c.underlying.Dbname
}

func (c *CoordShardInfo) Router() string {
	return c.router
}

func (c *CoordShardInfo) Usr() string {
	return c.underlying.Dbname
}

func (c *CoordShardInfo) InstanceHostname() string {
	return c.underlying.Hostname
}

func (c *CoordShardInfo) ID() uint {
	return uint(c.underlying.BackendConnectionId)
}

func (c *CoordShardInfo) ShardKeyName() string {
	return c.underlying.ShardKeyName
}

func (c *CoordShardInfo) Sync() int64 {
	return c.underlying.Sync
}

func (c *CoordShardInfo) TxServed() int64 {
	return c.underlying.TxServed
}

func (c *CoordShardInfo) TxStatus() txstatus.TXStatus {
	return txstatus.TXStatus(c.underlying.TxStatus)
}
