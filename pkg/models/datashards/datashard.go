package datashards

import (
	"github.com/pg-sharding/spqr/pkg/config"
	proto "github.com/pg-sharding/spqr/pkg/protos"
)

type DataShard struct {
	ID  string
	Cfg *config.Shard
}

func NewDataShard(name string, cfg *config.Shard) *DataShard {
	return &DataShard{
		ID:  name,
		Cfg: cfg,
	}
}

func DataShardToProto(shard *DataShard) *proto.Shard {
	return &proto.Shard{
		Hosts: shard.Cfg.Hosts,
		Id:    shard.ID,
	}
}
