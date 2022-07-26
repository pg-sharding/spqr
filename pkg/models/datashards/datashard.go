package datashards

import "github.com/pg-sharding/spqr/pkg/config"

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
