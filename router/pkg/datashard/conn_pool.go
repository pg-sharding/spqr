package datashard

import (
	"crypto/tls"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"math/rand"
	"sync"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
)

type Pool interface {
	Connection(shardKey kr.ShardKey, host string, rule *config.BERule) (Shard, error)
	Cut(host string) []Shard
	Put(host Shard) error
	List() []Shard
}

type cPool struct {
	mu   sync.Mutex
	pool map[string][]Shard

	f func(shardKey kr.ShardKey, host string, rule *config.BERule) (Shard, error)
}

func NewPool(f func(shardKey kr.ShardKey, host string, rule *config.BERule) (Shard, error)) *cPool {
	return &cPool{
		f:    f,
		mu:   sync.Mutex{},
		pool: map[string][]Shard{},
	}
}

func (c *cPool) Cut(host string) []Shard {
	var ret []Shard

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, instance := range c.pool[host] {
		ret = append(ret, instance)
	}

	c.pool[host] = nil

	return ret
}

func (c *cPool) List() []Shard {
	c.mu.Lock()
	defer c.mu.Unlock()

	var ret []Shard

	for _, llist := range c.pool {
		ret = append(ret, llist...)
	}

	return ret
}

func (c *cPool) Connection(shardKey kr.ShardKey, host string, rule *config.BERule) (Shard, error) {
	c.mu.Lock()

	var sh Shard

	if shds, ok := c.pool[host]; ok && len(shds) > 0 {
		sh, shds = shds[0], shds[1:]
		c.pool[host] = shds
		c.mu.Unlock()
		spqrlog.Logger.Printf(spqrlog.LOG, "got cached connection from pool")
		return sh, nil
	}
	c.mu.Unlock()

	// do not hold lock on poolRW while allocate new connection
	var err error
	sh, err = c.f(shardKey, host, rule)
	if err != nil {
		return nil, err
	}
	return sh, nil
}

func (c *cPool) Put(sh Shard) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pool[sh.Instance().Hostname()] = append(c.pool[sh.Instance().Hostname()], sh)
	return nil
}

var _ Pool = &cPool{}

type DBPool interface {
	Connection(key kr.ShardKey, rule *config.BERule) (Shard, error)
	Put(shkey kr.ShardKey, sh Shard) error

	Check(key kr.ShardKey) bool

	UpdateHostStatus(shard, hostname string, rw bool) error

	List() []Shard
}

type InstancePoolImpl struct {
	poolRW Pool
	poolRO Pool

	mu sync.Mutex

	primaries map[string]string

	tlsconfig *tls.Config
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

func (s *InstancePoolImpl) Check(key kr.ShardKey) bool {

	return true
	//
	//s.mu.Lock()
	//defer s.mu.Unlock()
	//
	//return len(s.poolRW[key]) > 0
}

func (s *InstancePoolImpl) List() []Shard {
	return append(s.poolRO.List(), s.poolRW.List()...)
}

var _ DBPool = &InstancePoolImpl{}

func (s *InstancePoolImpl) Connection(key kr.ShardKey, rule *config.BERule) (Shard, error) {
	switch key.RW {
	case true:
		var pr string
		var ok bool
		pr, ok = s.primaries[key.Name]
		if !ok {
			pr = config.RouterConfig().RulesConfig.ShardMapping[key.Name].Hosts[0].ConnAddr
		}

		shard, err := s.poolRW.Connection(key, pr, rule)
		if err != nil {
			return nil, err
		}

		return shard, nil
	case false:
		spqrlog.Logger.Printf(spqrlog.LOG, "acquire conn to %s", key.Name)
		hosts := config.RouterConfig().RulesConfig.ShardMapping[key.Name].Hosts
		rand.Shuffle(len(hosts), func(i, j int) {
			hosts[j], hosts[i] = hosts[i], hosts[j]
		})

		shard, err := s.poolRO.Connection(key, hosts[0].ConnAddr, rule)
		if err != nil {
			return nil, err
		}

		return shard, nil
	default:
		panic("never")
	}
}

func (s *InstancePoolImpl) Put(shkey kr.ShardKey, sh Shard) error {
	switch shkey.RW {
	case true:
		return s.poolRW.Put(sh)
	case false:
		return s.poolRO.Put(sh)
	default:
		panic("never")
	}
}

func NewConnPool(mapping map[string]*config.ShardCfg) DBPool {
	allocator := func(shardKey kr.ShardKey, host string, rule *config.BERule) (Shard, error) {
		var err error
		spqrlog.Logger.Printf(spqrlog.LOG, "acquire new connection to %v", host)
		var hostCfg *config.InstanceCFG

		for _, h := range config.RouterConfig().RulesConfig.ShardMapping[shardKey.Name].Hosts {
			if h.ConnAddr == host {
				hostCfg = h
			}
		}

		tlsconfig, err := mapping[shardKey.Name].TLSCfg.Init()
		if err != nil {
			return nil, err
		}

		pgi, err := conn.NewInstanceConn(hostCfg, tlsconfig)
		if err != nil {
			return nil, err
		}
		shardC, err := NewShard(shardKey, pgi, config.RouterConfig().RulesConfig.ShardMapping[shardKey.Name], rule)
		if err != nil {
			return nil, err
		}
		return shardC, nil
	}

	return &InstancePoolImpl{
		poolRW:    NewPool(allocator),
		poolRO:    NewPool(allocator),
		primaries: map[string]string{},
	}
}
