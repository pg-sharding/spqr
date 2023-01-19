package provider

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/config"

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
func (s *ShardServer) ListShards(ctx context.Context, _ *protos.ListShardRequest) (*protos.ListShardReply, error) {
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

	return &protos.ListShardReply{
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
