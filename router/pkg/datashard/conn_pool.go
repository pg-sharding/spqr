package datashard

import (
	"crypto/tls"
	"fmt"
	"math/rand"
	"net"
	"sync"

	"github.com/jackc/pgproto3/v2"

	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
)

type Pool interface {
	Connection(shardKey kr.ShardKey, host string, rule *config.BackendRule) (Shard, error)
	Cut(host string) []Shard
	Put(host Shard) error
	List() []Shard
}

type cPool struct {
	mu   sync.Mutex
	pool map[string][]Shard

	connectionAllocateFn func(shardKey kr.ShardKey, host string, rule *config.BackendRule) (Shard, error)
}

func NewPool(connectionAllocFn func(shardKey kr.ShardKey, host string, rule *config.BackendRule) (Shard, error)) *cPool {
	return &cPool{
		connectionAllocateFn: connectionAllocFn,
		mu:                   sync.Mutex{},
		pool:                 map[string][]Shard{},
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

func (c *cPool) Connection(shardKey kr.ShardKey, host string, rule *config.BackendRule) (Shard, error) {
	c.mu.Lock()

	var sh Shard

	if shds, ok := c.pool[host]; ok && len(shds) > 0 {
		sh, shds = shds[0], shds[1:]
		c.pool[host] = shds
		c.mu.Unlock()
		spqrlog.Logger.Printf(spqrlog.DEBUG1, "connection pool %p	: reuse cached shard connection %p", c, sh)
		return sh, nil
	}
	c.mu.Unlock()

	// do not hold lock on poolRW while allocate new connection
	var err error
	sh, err = c.connectionAllocateFn(shardKey, host, rule)
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
	Connection(key kr.ShardKey, rule *config.BackendRule) (Shard, error)
	Put(shkey kr.ShardKey, sh Shard) error

	Check(key kr.ShardKey) bool

	UpdateHostStatus(shard, hostname string, rw bool) error

	ShardMapping() map[string]*config.Shard

	List() []Shard
}

type InstancePoolImpl struct {
	poolRW Pool
	poolRO Pool

	mu sync.Mutex

	primaries map[string]string

	shardMapping map[string]*config.Shard

	tlsconfig *tls.Config
}

func (s *InstancePoolImpl) ShardMapping() map[string]*config.Shard {
	return s.shardMapping
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
	//s.mu.LockKeyRange()
	//defer s.mu.Unlock()
	//
	//return len(s.poolRW[key]) > 0
}

func (s *InstancePoolImpl) List() []Shard {
	return append(s.poolRO.List(), s.poolRW.List()...)
}

var _ DBPool = &InstancePoolImpl{}

func checkRw(sh Shard) (bool, error) {
	if err := sh.Send(&pgproto3.Query{
		String: "select pg_is_in_recovery()",
	}); err != nil {
		return false, err
	}

	res := false

	for {
		msg, err := sh.Receive()
		if err != nil {
			return false, err
		}
		switch qt := msg.(type) {
		case *pgproto3.DataRow:
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "shard %p checking read-write: result datarow %+v", sh, qt)
			if len(qt.Values) == 1 && len(qt.Values[0]) == 1 && qt.Values[0][0] == 'f' {
				res = true
			}

		case *pgproto3.ReadyForQuery:
			if conn.TXStatus(qt.TxStatus) != conn.TXIDLE {
				return false, fmt.Errorf("connection unsync while acquirind it")
			}
			return res, nil
		}
	}
}

func (s *InstancePoolImpl) Connection(key kr.ShardKey, rule *config.BackendRule) (Shard, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "instance pool %p: acquiring new instance conntion to shard %s", s, key.Name)

	hosts := s.shardMapping[key.Name].Hosts
	rand.Shuffle(len(hosts), func(i, j int) {
		hosts[j], hosts[i] = hosts[i], hosts[j]
	})

	switch s.shardMapping[key.Name].TargetSessionAttrs {
	case "":
		fallthrough
	case config.TargetSessionAttrsAny:

		shard, err := s.poolRO.Connection(key, hosts[0], rule)
		if err != nil {
			return nil, err
		}

		return shard, nil
	case config.TargetSessionAttrsRO:
		for _, host := range hosts {
			shard, err := s.poolRO.Connection(key, host, rule)
			if err != nil {
				return nil, err
			}
			if ch, err := checkRw(shard); err != nil {
				_ = shard.Close()
				continue
			} else if ch {
				_ = s.poolRO.Put(shard)
				continue
			}

			return shard, nil
		}
		return nil, fmt.Errorf("failed to find replica")
	case config.TargetSessionAttrsRW:
		for _, host := range hosts {
			shard, err := s.poolRO.Connection(key, host, rule)
			if err != nil {
				return nil, err
			}
			if ch, err := checkRw(shard); err != nil {
				_ = shard.Close()
				continue
			} else if !ch {
				_ = s.poolRO.Put(shard)
				continue
			}

			return shard, nil
		}
		return nil, fmt.Errorf("failed to find primary")
	default:
		return nil, fmt.Errorf("failed to match correct target session attrs")
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

func NewConnPool(mapping map[string]*config.Shard) DBPool {
	allocator := func(shardKey kr.ShardKey, host string, rule *config.BackendRule) (Shard, error) {
		shard := mapping[shardKey.Name]

		addr, _, _ := net.SplitHostPort(host)
		tlsconfig, err := shard.TLS.Init(addr)
		if err != nil {
			return nil, err
		}
		pgi, err := conn.NewInstanceConn(host, tlsconfig)
		if err != nil {
			return nil, err
		}
		shardC, err := NewShard(shardKey, pgi, mapping[shardKey.Name], rule)
		if err != nil {
			return nil, err
		}
		return shardC, nil
	}

	return &InstancePoolImpl{
		poolRW:       NewPool(allocator),
		poolRO:       NewPool(allocator),
		primaries:    map[string]string{},
		shardMapping: mapping,
	}
}
