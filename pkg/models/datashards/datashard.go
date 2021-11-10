package datashards

import "github.com/pg-sharding/spqr/pkg/config"

type DataShard struct {
	ID string

	cfg *config.ShardCfg
}

func NewDataShard(name string, cfg *config.ShardCfg) *DataShard {
	return &DataShard{
		ID:  name,
		cfg: cfg,
	}
}
