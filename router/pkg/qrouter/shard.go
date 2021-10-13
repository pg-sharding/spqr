package qrouter

import (
	"errors"

	"github.com/pg-sharding/spqr/coordinator/qdb/qdb"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/pkg/kr"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type ShardQrouter struct {
	shid string
}

var _ Qrouter = &ShardQrouter{}

func NewShardQrouter(shid string) (*ShardQrouter, error) {
	return &ShardQrouter{
		shid,
	}, nil
}

func (l *ShardQrouter) Subscribe(krid string, krst qdb.KeyRangeStatus, noitfyio chan<- interface{}) error {
	panic("implement me")
}

func (l *ShardQrouter) Unite(req *spqrparser.UniteKeyRange) error {
	panic("implement me")
}

func (l *ShardQrouter) AddLocalTable(tname string) error {
	return errors.New("local qrouter does not support sharding")
}

func (l *ShardQrouter) AddKeyRange(kr kr.KeyRange) error {
	return errors.New("local qrouter does not support sharding")
}

func (l *ShardQrouter) Shards() []string {
	return []string{l.shid}
}

func (l *ShardQrouter) KeyRanges() []kr.KeyRange {
	return nil
}

func (l *ShardQrouter) AddShard(name string, cfg *config.ShardCfg) error {
	return errors.New("local qrouter does not support sharding")
}

func (l *ShardQrouter) Lock(krid string) error {
	return errors.New("local qrouter does not support sharding")
}

func (l *ShardQrouter) UnLock(krid string) error {
	return errors.New("local qrouter does not support sharding")
}
func (l *ShardQrouter) Split(req *spqrparser.SplitKeyRange) error {
	return errors.New("local qrouter does not support sharding")
}
func (l *ShardQrouter) AddShardingColumn(col string) error {
	return errors.New("local qoruter does not supprort sharding")
}

func (l *ShardQrouter) Route(q string) []ShardRoute {
	return []ShardRoute{
		{
			Shkey: kr.ShardKey{
				Name: l.shid,
			},
		},
	}
}
