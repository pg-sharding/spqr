package internal

import (
	"log"
	"net"

	"github.com/pg-sharding/spqr/internal/config"
)

type Shard interface {
	Connect(proto string) (net.Conn, error)
	Cfg() *config.ShardCfg
	Name() string
}

type ShardImpl struct {
	cfg *config.ShardCfg

	lg log.Logger

	name string
}

func (sh *ShardImpl) Name() string {
	return sh.name
}

func (sh *ShardImpl) Cfg() *config.ShardCfg {
	return sh.cfg
}

func (sh *ShardImpl) Connect(proto string) (net.Conn, error) {
	return net.Dial(proto, sh.cfg.ConnAddr)
}

var _ Shard = &ShardImpl{}

func NewShard(name string, cfg *config.ShardCfg) Shard {
	return &ShardImpl{
		cfg:  cfg,
		name: name,
	}
}
