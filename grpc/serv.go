package shhttp

import (
	"context"

	shards "github.com/pg-sharding/spqr/router/protos"
)

type ShardServer struct {
	shards.UnimplementedShardServiceServer
}

func (s *ShardServer) AddShard(ctx context.Context, request *shards.AddShardRequest) (*shards.AddShardReply, error) {
	panic("implement me")
}

var _ shards.ShardServiceServer = &ShardServer{}

func (*ShardServer) ListShards(context.Context, *shards.ShardRequest) (*shards.ShardReply, error) {
	return &shards.ShardReply{
		Shards: []*shards.Shard{
			{
				Addr: "192.168.233.03",
				Id:   "1",
			},
			{
				Addr: "192.168.233.02",
				Id:   "2",
			},
		},
	}, nil
}

func (*ShardServer) GetShardInfo(_ context.Context, shardRequest *shards.ShardRequest) (*shards.ShardInfoReply, error) {
	if shardRequest.Id == "1" {
		return &shards.ShardInfoReply{
			ShardInfo: &shards.ShardInfo{
				Hosts: []string{
					"192.168.233.03",
				},
				Port: "6432",
			},
		}, nil
	}

	return &shards.ShardInfoReply{
		ShardInfo: &shards.ShardInfo{
			Hosts: []string{
				"192.168.233.02",
			},
			Port: "6432",
		},
	}, nil
}
