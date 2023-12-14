package pool

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

/* pool for single host */

type shardPool struct {
	mu   sync.Mutex
	pool []shard.Shard

	queue chan struct{}

	active map[uint]shard.Shard

	alloc ConnectionAllocFn

	beRule *config.BackendRule

	host string

	ConnectionLimit int
}

var _ Pool = &shardPool{}

func NewshardPool(allocFn ConnectionAllocFn, host string, beRule *config.BackendRule) Pool {
	connLimit := defaultInstanceConnectionLimit
	if beRule.ConnectionLimit != 0 {
		connLimit = beRule.ConnectionLimit
	}

	ret := &shardPool{
		mu:              sync.Mutex{},
		pool:            nil,
		active:          make(map[uint]shard.Shard),
		alloc:           allocFn,
		beRule:          beRule,
		host:            host,
		ConnectionLimit: connLimit,
	}

	ret.queue = make(chan struct{}, connLimit)
	for tok := 0; tok < connLimit; tok++ {
		ret.queue <- struct{}{}
	}

	spqrlog.Zero.Debug().
		Uint("pool", spqrlog.GetPointer(ret)).
		Int("tokens", connLimit).
		Msg("initialized pool queue with tokens")

	return ret
}

func (h *shardPool) Hostname() string {
	return h.host
}

func (h *shardPool) RouterName() string {
	return "unimplemented"
}

func (h *shardPool) Rule() *config.BackendRule {
	return h.beRule
}

func (h *shardPool) Cut(host string) []shard.Shard {
	h.mu.Lock()
	defer h.mu.Unlock()

	ret := h.pool
	h.pool = nil

	return ret
}

func (s *shardPool) UsedConnectionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active)
}

func (s *shardPool) IdleConnectionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pool)
}

func (s *shardPool) QueueResidualSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

func (h *shardPool) Connection(
	clid uint,
	shardKey kr.ShardKey) (shard.Shard, error) {

	if err := func() error {
		for rep := 0; rep < 10; rep++ {
			select {
			case <-time.After(50 * time.Millisecond * time.Duration(1+rand.Int31()%10)):
				spqrlog.Zero.Info().
					Uint("client", clid).
					Str("host", h.host).
					Msg("still waiting for backend connection to host")
			case <-h.queue:
				return nil
			}
		}

		return fmt.Errorf("failed to get connection to host %s due to too much concurrent connections", h.host)
	}(); err != nil {
		return nil, err
	}

	var sh shard.Shard

	/* reuse cached connection, if any */
	{
		/* TDB: per-bucket lock */
		h.mu.Lock()

		if len(h.pool) > 0 {
			sh, h.pool = h.pool[0], h.pool[1:]
			h.active[sh.ID()] = sh
			h.mu.Unlock()
			spqrlog.Zero.Debug().
				Uint("client", clid).
				Uint("shard", sh.ID()).
				Str("host", sh.Instance().Hostname()).
				Msg("connection pool for client: reuse cached shard connection to instance")
			return sh, nil
		}

		h.mu.Unlock()
	}

	// do not hold lock on poolRW while allocate new connection
	var err error
	sh, err = h.alloc(shardKey, h.host, h.beRule)
	if err != nil {
		return nil, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.active[sh.ID()] = sh

	return sh, nil
}

func (h *shardPool) Discard(sh shard.Shard) error {
	spqrlog.Zero.Debug().
		Uint("shard", sh.ID()).
		Str("host", sh.Instance().Hostname()).
		Msg("discard connection to hostname from pool")

	/* do not hold mutex while cleanup server connection */
	err := sh.Close()

	h.mu.Lock()
	defer h.mu.Unlock()

	/* acquired tok, release it */
	h.queue <- struct{}{}

	delete(h.active, sh.ID())

	return err
}

func (h *shardPool) Put(sh shard.Shard) error {
	spqrlog.Zero.Debug().
		Uint("shard", sh.ID()).
		Str("host", sh.Instance().Hostname()).
		Msg("put connection back to pool")

	if sh.TxStatus() != txstatus.TXIDLE {
		return h.Discard(sh)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	/* acquired tok, release it */
	h.queue <- struct{}{}

	delete(h.active, sh.ID())

	h.pool = append(h.pool, sh)
	return nil
}

func (h *shardPool) ForEach(cb func(sh shard.Shardinfo) error) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, sh := range h.pool {
		if err := cb(sh); err != nil {
			return err
		}
	}

	for _, sh := range h.active {
		if err := cb(sh); err != nil {
			return err
		}
	}
	return nil
}

func (h *shardPool) List() []shard.Shard {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.pool
}

/* pool with many hosts */

type cPool struct {
	PoolIterator
	Pool

	pools sync.Map

	alloc ConnectionAllocFn

	beRule *config.BackendRule
}

func NewPool(allocFn ConnectionAllocFn) MultiShardPool {
	return &cPool{
		pools: sync.Map{},
		alloc: allocFn,
	}
}

func (c *cPool) ForEach(cb func(sh shard.Shardinfo) error) error {
	c.pools.Range(func(key, value any) bool {
		_ = value.(Pool).ForEach(cb)
		return true
	})

	return nil
}
func (c *cPool) ForEachPool(cb func(p Pool) error) error {
	c.pools.Range(func(key, value any) bool {
		_ = cb(value.(Pool))
		return true
	})

	return nil
}

func (c *cPool) List() []shard.Shard {
	var ret []shard.Shard

	c.pools.Range(func(key, value any) bool {
		ret = append(ret, value.(Pool).List()...)
		return true
	})
	return ret
}

func (c *cPool) Connection(clid uint, shardKey kr.ShardKey, host string) (shard.Shard, error) {
	var pool Pool
	if val, ok := c.pools.Load(host); !ok {
		pool = NewshardPool(c.alloc, host, c.beRule)
		c.pools.Store(host, pool)
	} else {
		pool = val.(Pool)
	}
	return pool.Connection(clid, shardKey)
}

func (c *cPool) Cut(host string) []shard.Shard {
	rt, _ := c.pools.LoadAndDelete(host)
	return rt.([]shard.Shard)
}

func (c *cPool) Put(host shard.Shard) error {
	if val, ok := c.pools.Load(host.Instance().Hostname()); ok {
		return val.(Pool).Put(host)
	} else {
		/* very bad */
		panic(host)
	}
}

func (c *cPool) Discard(sh shard.Shard) error {
	if val, ok := c.pools.Load(sh.Instance().Hostname()); ok {
		return val.(Pool).Discard(sh)
	} else {
		/* very bad */
		panic(sh)
	}
}

func (c *cPool) InitRule(rule *config.BackendRule) error {
	c.beRule = rule
	return nil
}

var _ MultiShardPool = &cPool{}
