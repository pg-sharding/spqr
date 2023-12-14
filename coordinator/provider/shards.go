package provider

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/config"
	routerproto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	protos "github.com/pg-sharding/spqr/pkg/protos"
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

func (s *ShardServer) AddDataShard(ctx context.Context, request *protos.AddShardRequest) (*protos.AddShardReply, error) {
	newShard := request.GetShard()

	if err := s.impl.AddDataShard(ctx, datashards.NewDataShard(newShard.Id, &config.Shard{
		Hosts: newShard.Hosts,
	})); err != nil {
		return nil, err
	}

	return &protos.AddShardReply{}, nil
}

func (s *ShardServer) AddWorldShard(ctx context.Context, request *protos.AddWorldShardRequest) (*protos.AddShardReply, error) {
	panic("implement me")
}

// TODO: remove ShardRequest.
func (s *ShardServer) ListShards(ctx context.Context, _ *protos.ListShardsRequest) (*protos.ListShardsReply, error) {
	shardList, err := s.impl.ListShards(ctx)
	if err != nil {
		return nil, err
	}

	protoShards := make([]*protos.Shard, 0, len(shardList))

	for _, shard := range shardList {
		protoShards = append(protoShards, &protos.Shard{
			Hosts: shard.Cfg.Hosts,
			Id:    shard.ID,
		})
	}

	return &protos.ListShardsReply{
		Shards: protoShards,
	}, nil
}

func (s *ShardServer) GetShardInfo(ctx context.Context, shardRequest *protos.ShardRequest) (*protos.ShardInfoReply, error) {
	shardInfo, err := s.impl.GetShardInfo(ctx, shardRequest.Id)
	if err != nil {
		return nil, err
	}

	return &protos.ShardInfoReply{
		ShardInfo: &protos.ShardInfo{
			Hosts: shardInfo.Cfg.Hosts,
			Id:    shardInfo.ID,
		},
	}, nil
}

type CoordShardInfo struct {
	underlying *routerproto.BackendConnectionsInfo
	router     string
}

func NewCoordShardInfo(conn *routerproto.BackendConnectionsInfo, router string) shard.Shardinfo {
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
