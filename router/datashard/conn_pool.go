package datashard

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

const defaultInstanceConnectionLimit = 50

type Pool interface {
	Connection(clid string, shardKey kr.ShardKey, host string, rule *config.BackendRule) (Shard, error)
	Cut(host string) []Shard
	Put(host Shard) error
	List() []Shard
}

type cPool struct {
	mu   sync.Mutex
	pool map[string][]Shard

	queues map[string]chan struct{}

	connectionAllocateFn func(shardKey kr.ShardKey, host string, rule *config.BackendRule) (Shard, error)
}

func NewPool(connectionAllocFn func(shardKey kr.ShardKey, host string, rule *config.BackendRule) (Shard, error)) *cPool {
	return &cPool{
		connectionAllocateFn: connectionAllocFn,
		mu:                   sync.Mutex{},
		pool:                 map[string][]Shard{},
		queues:               map[string]chan struct{}{},
	}
}

func (c *cPool) Cut(host string) []Shard {
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := append([]Shard{}, c.pool[host]...)
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

func (c *cPool) Connection(clid string, shardKey kr.ShardKey, host string, rule *config.BackendRule) (Shard, error) {
	connLimit := defaultInstanceConnectionLimit
	if rule.ConnectionLimit != 0 {
		connLimit = rule.ConnectionLimit
	}

	/* create channels for host, if not yet */
	{
		c.mu.Lock()

		if _, ok := c.queues[host]; !ok {
			c.queues[host] = make(chan struct{}, connLimit)
			for tok := 0; tok < connLimit; tok++ {
				c.queues[host] <- struct{}{}
			}
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG5, "initialized %p pool queue with %d tokens", c, connLimit)

		c.mu.Unlock()
	}

	if err := func() error {
		for rep := 0; rep < 10; rep++ {
			select {
			case <-time.After(50 * time.Millisecond):
				spqrlog.Logger.ClientErrorf("still waiting for backend connection to host %s", clid, host)
			case <-c.queues[host]:
				return nil
			}
		}

		return fmt.Errorf("failed to get connection to host %s due to too much concurrent conections", host)
	}(); err != nil {
		return nil, err
	}

	var sh Shard

	/* reuse cached connection, if any */
	{
		c.mu.Lock()

		if shds, ok := c.pool[host]; ok && len(shds) > 0 {
			sh, shds = shds[0], shds[1:]
			c.pool[host] = shds
			c.mu.Unlock()
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "connection pool for client %s: reuse cached shard connection %p to %s", clid, sh, sh.Instance().Hostname())
			return sh, nil
		}

		c.mu.Unlock()
	}

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

	/* acquired tok, release it */
	defer func() {
		c.queues[sh.Instance().Hostname()] <- struct{}{}
	}()

	c.pool[sh.Instance().Hostname()] = append(c.pool[sh.Instance().Hostname()], sh)
	return nil
}

var _ Pool = &cPool{}

type DBPool interface {
	Connection(clid string, key kr.ShardKey, rule *config.BackendRule, tsa string) (Shard, error)
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
		spqrlog.Logger.Errorf("shard %s encounter error while sending read-write check %v", sh.Name(), err)
		return false, err
	}

	res := false

	for {
		msg, err := sh.Receive()
		if err != nil {
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "shard %p recieved error %v during check rw", sh, err)
			return false, err
		}
		spqrlog.Logger.Printf(spqrlog.DEBUG5, "shard %p recieved %+v during check rw", sh, msg)
		switch qt := msg.(type) {
		case *pgproto3.DataRow:
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "shard %p checking read-write: result datarow %+v", sh, qt)
			if len(qt.Values) == 1 && len(qt.Values[0]) == 1 && qt.Values[0][0] == 'f' {
				res = true
			}

		case *pgproto3.ReadyForQuery:
			if txstatus.TXStatus(qt.TxStatus) != txstatus.TXIDLE {
				spqrlog.Logger.Printf(spqrlog.DEBUG5, "shard %p got unsync connection while calculating rw %v", sh, qt.TxStatus)
				return false, fmt.Errorf("connection unsync while acquirind it")
			}

			spqrlog.Logger.Printf(spqrlog.DEBUG5, "shard %p calculated rw res %+v", sh, res)
			return res, nil
		}
	}
}

func (s *InstancePoolImpl) Connection(clid string, key kr.ShardKey, rule *config.BackendRule, TargetSessionAttrs string) (Shard, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "acquiring new instance connection for client '%s' to shard '%s' with tsa: '%s'", clid, key.Name, TargetSessionAttrs)

	hosts := make([]string, len(s.shardMapping[key.Name].Hosts))
	copy(hosts, s.shardMapping[key.Name].Hosts)
	rand.Shuffle(len(hosts), func(i, j int) {
		hosts[j], hosts[i] = hosts[i], hosts[j]
	})

	switch TargetSessionAttrs {
	case "":
		fallthrough
	case config.TargetSessionAttrsAny:
		total_msg := ""
		for _, host := range hosts {
			shard, err := s.poolRO.Connection(clid, key, host, rule)
			if err != nil {
				total_msg += err.Error()
				spqrlog.Logger.Errorf("failed to get connection to %s for %s: %v", host, clid, err)
				continue
			}
			return shard, nil
		}
		return nil, fmt.Errorf("failed to get connection to any shard host within %+v: %s", hosts, total_msg)
	case config.TargetSessionAttrsRO:
		total_msg := ""

		for _, host := range hosts {
			shard, err := s.poolRO.Connection(clid, key, host, rule)
			if err != nil {
				total_msg += err.Error()
				spqrlog.Logger.Errorf("failed to get connection to %s for %s: %v", host, clid, err)
				continue
			}
			if ch, err := checkRw(shard); err != nil {
				total_msg += err.Error()
				_ = shard.Close()
				continue
			} else if ch {
				_ = s.poolRO.Put(shard)
				continue
			}

			return shard, nil
		}
		return nil, fmt.Errorf("shard %s failed to find replica within %+v: %s", key.Name, hosts, total_msg)
	case config.TargetSessionAttrsRW:
		total_msg := ""
		for _, host := range hosts {
			shard, err := s.poolRO.Connection(clid, key, host, rule)
			if err != nil {
				total_msg += err.Error()
				spqrlog.Logger.Errorf("failed to get connection to %s for %s: %v", host, clid, err)
				continue
			}
			if ch, err := checkRw(shard); err != nil {
				total_msg += err.Error()
				_ = shard.Close()
				continue
			} else if !ch {
				_ = s.poolRO.Put(shard)
				continue
			}

			return shard, nil
		}
		return nil, fmt.Errorf("shard %s failed to find primary within %+v: %s", key.Name, hosts, total_msg)
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
