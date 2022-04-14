package datashards

import "context"

type ShardsMgr interface {
	ListDataShards(ctx context.Context) []*DataShard
	AddDataShard(ctx context.Context, ds *DataShard) error
}

type ShardsManager interface {
	AddDataShard(ctx context.Context, shard *Shard) error
	AddWorldShard(ctx context.Context, shard *Shard) error
	ListShards(ctx context.Context) ([]*Shard, error)
	GetShardInfo(ctx context.Context, shardID string) (*ShardInfo, error)
}
