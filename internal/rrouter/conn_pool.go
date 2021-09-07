package rrouter

import (
	"crypto/tls"
	"sync"

	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/conn"
	"github.com/pg-sharding/spqr/internal/qrouterdb"
	"github.com/wal-g/tracelog"
)

type Pool interface {
	Connection(key string) (conn.DBInstance, error)
	Cut(key string) []conn.DBInstance
	Put(sh conn.DBInstance) error
	List() []conn.DBInstance
}

type cPool struct {
	mu   sync.Mutex
	pool map[string][]conn.DBInstance

	mapping map[string]*config.ShardCfg
}

func (c *cPool) Cut(key string) []conn.DBInstance {
	var ret []conn.DBInstance

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, instance := range c.pool[key] {
		ret = append(ret, instance)
	}

	c.pool[key] = nil

	return ret
}

func (c *cPool) List() []conn.DBInstance {
	c.mu.Lock()
	defer c.mu.Unlock()

	var ret []conn.DBInstance

	for _, llist := range c.pool {
		ret = append(ret, llist...)
	}

	return ret
}

func (c *cPool) Connection(key string) (conn.DBInstance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var sh conn.DBInstance

	if shds, ok := c.pool[key]; ok && len(shds) > 0 {
		sh, shds = shds[0], shds[1:]
		c.pool[key] = shds
		return sh, nil
	}

	// do not hold lock on poolRW while allocate new connection
	c.mu.Unlock()
	{
		tracelog.InfoLogger.Printf("acquire new connection to %v", key)

		cfg := c.mapping[key]

		var err error
		sh, err = conn.NewInstanceConn(cfg.Hosts[0], c.mapping[key].TLSConfig, cfg.TLSCfg.SslMode)
		if err != nil {
			return nil, err
		}
	}
	c.mu.Lock()

	return sh, nil
}

func (c *cPool) Put(sh conn.DBInstance) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pool[sh.Hostname()] = append(c.pool[sh.Hostname()], sh)

	return nil
}

func NewPool(mapping map[string]*config.ShardCfg) *cPool {
	return &cPool{
		mu:      sync.Mutex{},
		pool:    map[string][]conn.DBInstance{},
		mapping: mapping,
	}
}

var _ Pool = &cPool{}

type ConnPool interface {
	Connection(key qrouterdb.ShardKey) (conn.DBInstance, error)
	Put(shkey qrouterdb.ShardKey, sh conn.DBInstance) error

	Check(key qrouterdb.ShardKey) bool

	UpdateHostStatus(shard, hostname string, rw bool) error

	List() []conn.DBInstance
}

type InstancePoolImpl struct {
	poolRW Pool
	poolRO Pool

	mu sync.Mutex

	primaries map[string]string

	tlscfg *tls.Config
}

func (s *InstancePoolImpl) UpdateHostStatus(shard, hostname string, rw bool) error {

	s.mu.Lock()

	src := s.poolRW
	dest := s.poolRO

	if rw {
		src, dest = dest, src
		s.primaries[shard] = hostname
	} else {
		delete(s.primaries, shard)
	}

	s.mu.Unlock()

	for _, instance := range src.Cut(hostname) {
		_ = dest.Put(instance)
	}

	return nil
}

func (s *InstancePoolImpl) Check(key qrouterdb.ShardKey) bool {

	return true
	//
	//s.mu.Lock()
	//defer s.mu.Unlock()
	//
	//return len(s.poolRW[key]) > 0
}

func (s *InstancePoolImpl) List() []conn.DBInstance {
	return append(s.poolRO.List(), s.poolRW.List()...)
}

var _ ConnPool = &InstancePoolImpl{}

func (s *InstancePoolImpl) Connection(key qrouterdb.ShardKey) (conn.DBInstance, error) {

	switch key.RW {
	case true:
		return s.poolRW.Connection(key.Name)
	case false:
		return s.poolRO.Connection(key.Name)
	default:
		panic("never")
	}

}

func (s *InstancePoolImpl) Put(shkey qrouterdb.ShardKey, sh conn.DBInstance) error {

	switch shkey.RW {
	case true:
		return s.poolRW.Put(sh)
	case false:
		return s.poolRO.Put(sh)
	default:
		panic("never")
	}
}

func NewConnPool(mapping map[string]*config.ShardCfg) ConnPool {
	return &InstancePoolImpl{
		poolRW:    NewPool(mapping),
		poolRO:    NewPool(mapping),
		primaries: map[string]string{},
	}
}
