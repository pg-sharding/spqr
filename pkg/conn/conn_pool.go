package conn

import (
	"crypto/tls"
	"math/rand"
	"sync"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/wal-g/tracelog"
)

type Pool interface {
	Connection(shard, host string) (DBInstance, error)
	Cut(host string) []DBInstance
	Put(host DBInstance) error
	List() []DBInstance
}

type cPool struct {
	mu   sync.Mutex
	pool map[string][]DBInstance

	mapping map[string]*config.ShardCfg
}

func (c *cPool) Cut(host string) []DBInstance {
	var ret []DBInstance

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, instance := range c.pool[host] {
		ret = append(ret, instance)
	}

	c.pool[host] = nil

	return ret
}

func (c *cPool) List() []DBInstance {
	c.mu.Lock()
	defer c.mu.Unlock()

	var ret []DBInstance

	for _, llist := range c.pool {
		ret = append(ret, llist...)
	}

	return ret
}

func (c *cPool) Connection(shard, host string) (DBInstance, error) {
	c.mu.Lock()

	var sh DBInstance

	if shds, ok := c.pool[host]; ok && len(shds) > 0 {
		sh, shds = shds[0], shds[1:]
		c.pool[host] = shds
		c.mu.Unlock()
		tracelog.InfoLogger.Printf("got cached connection from pool")
		return sh, nil
	}
	c.mu.Unlock()

	// do not hold lock on poolRW while allocate new connection

	tracelog.InfoLogger.Printf("acquire new connection to %v", host)
	var hostCfg *config.InstanceCFG

	for _, h := range config.RouterConfig().RulesConfig.ShardMapping[shard].Hosts {
		if h.ConnAddr == host {
			hostCfg = h
		}
	}

	var err error

	sh, err = NewInstanceConn(hostCfg, c.mapping[shard].TLSConfig, c.mapping[shard].TLSCfg.SslMode)
	if err != nil {
		return nil, err
	}
	return sh, nil
}

func (c *cPool) Put(sh DBInstance) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pool[sh.Hostname()] = append(c.pool[sh.Hostname()], sh)

	return nil
}

func NewPool(mapping map[string]*config.ShardCfg) *cPool {
	return &cPool{
		mu:      sync.Mutex{},
		pool:    map[string][]DBInstance{},
		mapping: mapping,
	}
}

var _ Pool = &cPool{}

type ConnPool interface {
	Connection(key kr.ShardKey) (DBInstance, error)
	Put(shkey kr.ShardKey, sh DBInstance) error

	Check(key kr.ShardKey) bool

	UpdateHostStatus(shard, hostname string, rw bool) error

	List() []DBInstance
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

func (s *InstancePoolImpl) Check(key kr.ShardKey) bool {

	return true
	//
	//s.mu.Lock()
	//defer s.mu.Unlock()
	//
	//return len(s.poolRW[key]) > 0
}

func (s *InstancePoolImpl) List() []DBInstance {
	return append(s.poolRO.List(), s.poolRW.List()...)
}

var _ ConnPool = &InstancePoolImpl{}

func (s *InstancePoolImpl) Connection(key kr.ShardKey) (DBInstance, error) {

	switch key.RW {
	case true:
		var pr string
		var ok bool
		pr, ok = s.primaries[key.Name]
		if !ok {
			pr = config.RouterConfig().RulesConfig.ShardMapping[key.Name].Hosts[0].ConnAddr
		}
		return s.poolRW.Connection(key.Name, pr)
	case false:
		tracelog.InfoLogger.Printf("get conn to %s", key.Name)
		hosts := config.RouterConfig().RulesConfig.ShardMapping[key.Name].Hosts
		rand.Shuffle(len(hosts), func(i, j int) {
			hosts[j], hosts[i] = hosts[i], hosts[j]
		})
		return s.poolRO.Connection(key.Name, hosts[0].ConnAddr)
	default:
		panic("never")
	}

}

func (s *InstancePoolImpl) Put(shkey kr.ShardKey, sh DBInstance) error {

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
