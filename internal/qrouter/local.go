package qrouter

import (
	"errors"

	"github.com/pg-sharding/spqr/coordinator/qdb/qdb"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/yacc/console"
)

type LocalQrouter struct {
	shid string
}

var _ Qrouter = &LocalQrouter{}

func NewLocalQrouter(shid string) (*LocalQrouter, error) {
	return &LocalQrouter{
		shid,
	}, nil
}

func (l *LocalQrouter) Subscribe(krid string, krst qdb.KeyRangeStatus, noitfyio chan<- interface{}) error {
	panic("implement me")
}

func (l *LocalQrouter) Unite(req *spqrparser.UniteKeyRange) error {
	panic("implement me")
}

func (l *LocalQrouter) AddLocalTable(tname string) error {
	return errors.New("local qrouter does not support sharding")
}

func (l *LocalQrouter) AddKeyRange(kr qdb.KeyRange) error {
	return errors.New("local qrouter does not support sharding")
}

func (l *LocalQrouter) Shards() []string {
	return []string{l.shid}
}

func (l *LocalQrouter) KeyRanges() []qdb.KeyRange {
	return nil
}

func (l *LocalQrouter) AddShard(name string, cfg *config.ShardCfg) error {
	return errors.New("local qrouter does not support sharding")
}

func (l *LocalQrouter) Lock(krid string) error {
	return errors.New("local qrouter does not support sharding")
}

func (l *LocalQrouter) UnLock(krid string) error {
	return errors.New("local qrouter does not support sharding")
}

func (l *LocalQrouter) Split(req *spqrparser.SplitKeyRange) error {
	return errors.New("local qrouter does not support sharding")
}

func (l *LocalQrouter) AddShardingColumn(col string) error {
	return errors.New("local qoruter does not supprort sharding")
}

func (l *LocalQrouter) Route(q string) []ShardRoute {
	return []ShardRoute{{Shkey: qdb.ShardKey{
		Name: l.shid,
	},
	},
	}
}
