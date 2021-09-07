package rrouter

import (
	"sync"

	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/qrouterdb"
	"github.com/wal-g/tracelog"
)

type ShardPool interface {
	Connection(key qrouterdb.ShardKey) (Shard, error)
	Put(sh Shard) error

	Check(key qrouterdb.ShardKey) bool

	List() []Shard
}

type ShardPoolImpl struct {
	mu sync.Mutex

	poolRW map[qrouterdb.ShardKey][]Shard
	poolRO map[qrouterdb.ShardKey][]Shard

	mapping map[string]*config.ShardCfg
}

func (s *ShardPoolImpl) Check(key qrouterdb.ShardKey) bool {

	return true
	//
	//s.mu.Lock()
	//defer s.mu.Unlock()
	//
	//return len(s.poolRW[key]) > 0
}

func (s *ShardPoolImpl) List() []Shard {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ret []Shard

	for _, shl := range s.poolRW {
		for _, sh := range shl {
			ret = append(ret, sh)
		}
	}

	return ret
}

var _ ShardPool = &ShardPoolImpl{}

func (s *ShardPoolImpl) Connection(key qrouterdb.ShardKey) (Shard, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var sh Shard

	if shds, ok := s.poolRW[key]; ok && len(shds) > 0 {
		sh, shds = shds[0], shds[1:]
		s.poolRW[key] = shds
		return sh, nil
	}

	// do not hold lock on poolRW while allocate new connection
	s.mu.Unlock()
	{
		tracelog.InfoLogger.Printf("acquire new connection to %v", key)

		cfg := s.mapping[key.Name]

		var err error
		sh, err = NewShard(key, cfg.Hosts[0].ConnAddr, cfg)
		if err != nil {
			return nil, err
		}
	}
	s.mu.Lock()

	return sh, nil
}

func (s *ShardPoolImpl) Put(sh Shard) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.poolRW[sh.SHKey()] = append(s.poolRW[sh.SHKey()], sh)

	return nil
}

func NewShardPool(mapping map[string]*config.ShardCfg) ShardPool {
	return &ShardPoolImpl{
		mu:      sync.Mutex{},
		mapping: mapping,
		poolRW:  map[qrouterdb.ShardKey][]Shard{},
	}
}
