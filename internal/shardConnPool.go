package internal

import (
	"sync"

	"github.com/pg-sharding/spqr/internal/config"
)

type ShardPool interface {
	Connection(key ShardKey) (Shard, error)
	Put(sh Shard) error

	Check(key ShardKey) bool

	List() []Shard
}

type ShardPoolImpl struct {
	mu sync.Mutex

	pool map[ShardKey][]Shard

	mapping map[string]*config.ShardCfg
}

func (s *ShardPoolImpl) Check(key ShardKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.pool[key]) > 0
}

func (s *ShardPoolImpl) List() []Shard {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ret []Shard

	for _, shl := range s.pool {
		for _, sh := range shl {
			ret = append(ret, sh)
		}
	}

	return ret
}

var _ ShardPool = &ShardPoolImpl{}

func (s *ShardPoolImpl) Connection(key ShardKey) (Shard, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var sh Shard

	if shds, ok := s.pool[key]; ok && len(shds) > 0 {
		sh, shds = shds[0], shds[1:]
		s.pool[key] = shds
	} else {
		var err error
		sh, err = NewShard(key.name, s.mapping[key.name])
		if err != nil {
			return nil, err
		}
	}

	return sh, nil
}

func (s *ShardPoolImpl) Put(sh Shard) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pool[sh.SHKey()] = append(s.pool[sh.SHKey()], sh)

	return nil
}

func NewShardPool(mapping map[string]*config.ShardCfg) ShardPool {
	return &ShardPoolImpl{
		mu:      sync.Mutex{},
		mapping: mapping,
		pool:    map[ShardKey][]Shard{},
	}
}
