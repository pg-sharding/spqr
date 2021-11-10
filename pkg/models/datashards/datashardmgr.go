package datashards

import "context"

type DataShardsMgr interface {
	ListDataShards(ctx context.Context) []*DataShard
	AddDataShard(ctx context.Context, ds *DataShard) error
}
