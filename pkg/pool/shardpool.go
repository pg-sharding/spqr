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

	ConnectionLimit            int
	ConnectionRetries          int
	ConnectionRetrySleepSlice  int
	ConnectionRetryRandomSleep int
}

var _ Pool = &shardPool{}

// NewShardPool creates a new instance of shardPool based on the provided ConnectionAllocFn, host, and BackendRule.
// It initializes the fields of shardPool with the corresponding values from the ConnectionAllocFn and BackendRule.
// It also initializes the queue with the ConnectionLimit.
//
// Parameters:
//   - allocFn: The ConnectionAllocFn to be used for the shardPool.
//   - host: The host to be used for the shardPool.
//   - beRule: The BackendRule to be used for the shardPool.
//
// Returns:
//   - Pool: The created instance of shardPool.
func NewShardPool(allocFn ConnectionAllocFn, host string, beRule *config.BackendRule) Pool {
	connLimit := defaultInstanceConnectionLimit
	connRetries := defaultInstanceConnectionRetries
	if beRule.ConnectionLimit != 0 {
		connLimit = beRule.ConnectionLimit
	}

	if beRule.ConnectionRetries != 0 {
		connRetries = beRule.ConnectionRetries
	}

	ret := &shardPool{
		mu:                         sync.Mutex{},
		pool:                       nil,
		active:                     make(map[uint]shard.Shard),
		alloc:                      allocFn,
		beRule:                     beRule,
		host:                       host,
		ConnectionLimit:            connLimit,
		ConnectionRetries:          connRetries,
		ConnectionRetrySleepSlice:  50,
		ConnectionRetryRandomSleep: 10,
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

// Hostname returns the hostname of the shardPool.
//
// Returns:
//   - string: The hostname of the shardPool.
func (h *shardPool) Hostname() string {
	return h.host
}

// RouterName returns the name of the router.
//
// Returns:
//   - string: The name of the router.
func (h *shardPool) RouterName() string {
	return "unimplemented"
}

// Rule returns the backend rule associated with the shard pool.
//
// Returns:
//   - *config.BackendRule: The backend rule associated with the shard pool.
func (h *shardPool) Rule() *config.BackendRule {
	return h.beRule
}

// Cut removes all shards from the shard pool and returns them as a slice.
//
// Parameters:
//   - host: The host from which to cut the shards.
//
// Returns:
//   - []shard.Shard: The removed shards as a slice.
func (h *shardPool) Cut(host string) []shard.Shard {
	h.mu.Lock()
	defer h.mu.Unlock()

	ret := h.pool
	h.pool = nil

	return ret
}

// UsedConnectionCount returns the number of currently used connections in the shard pool.
//
// Returns:
//   - int: The number of currently used connections in the shard pool.
func (s *shardPool) UsedConnectionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active)
}

// IdleConnectionCount returns the number of idle connections in the shard pool.
//
// Returns:
//   - int: The number of idle connections in the shard pool.
func (s *shardPool) IdleConnectionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pool)
}

// QueueResidualSize returns the number of elements in the queue of the shard pool.
//
// Returns:
//   - int: The number of elements in the queue of the shard pool.
func (s *shardPool) QueueResidualSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

// Connection retrieves a connection to a shard based on the provided client ID and shard key.
// It first attempts to retrieve a connection from the connection pool. If a connection is available,
// it is reused. If no connection is available, it waits for a connection to become available or
// retries based on the configured connection retries and sleep duration.
// If a connection cannot be obtained within the configured retries, an error is returned.
// If a connection is successfully obtained, it is added to the active connections and returned.
// If an error occurs during the allocation of a new connection, the acquired token is returned
// and an error is returned.
//
// Parameters:
//   - clid: The client ID to be used for the connection.
//   - shardKey: The ShardKey to be used for the connection.
//
// Returns:
//   - shard.Shard: The obtained connection to the shard.
//   - error: The error that occurred during the connection process.
//
// TODO : unit tests
func (h *shardPool) Connection(
	clid uint,
	shardKey kr.ShardKey) (shard.Shard, error) {

	if err := func() error {
		for rep := 0; rep < h.ConnectionRetries; rep++ {
			select {
			// TODO: configure waits using backend rule
			case <-time.After(time.Duration(h.ConnectionRetrySleepSlice) * time.Millisecond * time.Duration(1+rand.Int31()%int32(h.ConnectionRetryRandomSleep))):
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
		// return acquired token
		h.queue <- struct{}{}
		return nil, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.active[sh.ID()] = sh

	return sh, nil
}

// Discard removes a shard from the shard pool and closes the connection to the shard's host.
// It returns an error if there was an issue closing the connection.
//
// Parameters:
//   - sh: The shard to be removed from the shard pool.
//
// Returns:
//   - error: The error that occurred during the removal of the shard.
//
// TODO : unit tests
func (h *shardPool) Discard(sh shard.Shard) error {
	spqrlog.Zero.Debug().
		Uint("shard", sh.ID()).
		Str("host", sh.Instance().Hostname()).
		Msg("discard connection to hostname from pool")

	/* do not hold mutex while cleanup server connection */
	err := sh.Close()

	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.active[sh.ID()]; !ok {
		// double free
		return nil
	}

	/* acquired tok, release it */
	h.queue <- struct{}{}

	delete(h.active, sh.ID())

	return err
}

// Put puts the given shard back into the shard pool.
// It first checks if the shard's transaction status is idle.
// If not, it discards the shard.
// Then, it removes the shard from the active map and adds it back to the pool.
//
// Parameters:
//   - sh: The shard to be put back into the shard pool.
//
// Returns:
//   - error: The error that occurred during the put operation.
//
// TODO : unit tests
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

	if _, ok := h.active[sh.ID()]; !ok {
		// double free
		panic(sh)
	}

	/* acquired tok, release it */
	h.queue <- struct{}{}

	delete(h.active, sh.ID())

	h.pool = append(h.pool, sh)
	return nil
}

// ForEach iterates over each shard in the shard pool and calls the provided callback function.
// If the callback function returns an error, the iteration stops and the error is returned.
//
// Parameters:
//   - cb: The callback function to be called for each shard.
//
// Returns:
//   - error: The error that occurred during the iteration.
//
// TODO : unit tests
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

// List returns a slice of shards in the shard pool.
//
// Returns:
//   - []shard.Shard: The slice of shards in the shard pool.
//
// TODO : unit tests
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

// NewPool creates a new MultiShardPool with the given connection allocation function.
// The connection allocation function allocFn is used to allocate connections for each shard.
// It returns a pointer to the created MultiShardPool.
//
// Parameters:
//   - allocFn: The connection allocation function to be used for the MultiShardPool.
//
// Returns:
//   - MultiShardPool: The created MultiShardPool.
func NewPool(allocFn ConnectionAllocFn) MultiShardPool {
	return &cPool{
		pools: sync.Map{},
		alloc: allocFn,
	}
}

// ForEach iterates over each shard in the shard pool and calls the provided callback function.
// If the callback function returns an error, the iteration stops and the error is returned.
//
// Parameters:
//   - cb: The callback function to be called for each shard.
//
// Returns:
//   - error: The error that occurred during the iteration.
//
// TODO : unit tests
func (c *cPool) ForEach(cb func(sh shard.Shardinfo) error) error {
	c.pools.Range(func(key, value any) bool {
		_ = value.(Pool).ForEach(cb)
		return true
	})

	return nil
}

// ForEachPool iterates over each pool in the cPool and calls the provided callback function.
// The callback function takes a Pool as a parameter and returns an error.
// If the callback function returns an error, the iteration stops and the error is returned.
//
// Parameters:
//   - cb: The callback function to be called for each pool.
//
// Returns:
//   - error: The error that occurred during the iteration.
//
// TODO : unit tests
func (c *cPool) ForEachPool(cb func(p Pool) error) error {
	c.pools.Range(func(key, value any) bool {
		_ = cb(value.(Pool))
		return true
	})

	return nil
}

// List returns a slice of shard.Shard containing all the shards in the cPool.
//
// Returns:
//   - []shard.Shard: The slice of shard.Shard containing all the shards in the cPool.
//
// TODO : unit tests
func (c *cPool) List() []shard.Shard {
	var ret []shard.Shard

	c.pools.Range(func(key, value any) bool {
		ret = append(ret, value.(Pool).List()...)
		return true
	})
	return ret
}

// Connection returns a shard connection for the given client ID, shard key, and host.
// If a connection pool for the host does not exist, a new pool is created and stored.
// Otherwise, the existing pool is retrieved from the cache.
// The method then delegates the connection retrieval to the pool and returns the result.
//
// Parameters:
//   - clid: The client ID to be used for the connection.
//   - shardKey: The ShardKey to be used for the connection.
//   - host: The host to be used for the connection.
//
// Returns:
//   - shard.Shard: The obtained connection to the shard.
//   - error: The error that occurred during the connection process.
//
// TODO : unit tests
func (c *cPool) Connection(clid uint, shardKey kr.ShardKey, host string) (shard.Shard, error) {
	var pool Pool
	if val, ok := c.pools.Load(host); !ok {
		pool = NewShardPool(c.alloc, host, c.beRule)
		c.pools.Store(host, pool)
	} else {
		pool = val.(Pool)
	}
	return pool.Connection(clid, shardKey)
}

// Cut removes and returns the shards associated with the specified host.
// It returns a slice of shard.Shard.
//
// Parameters:
//   - host: The host for which to cut the shards.
//
// Returns:
//   - []shard.Shard: The removed shards as a slice.
//
// TODO : unit tests
func (c *cPool) Cut(host string) []shard.Shard {
	rt, _ := c.pools.LoadAndDelete(host)
	return rt.([]shard.Shard)
}

// Put adds a shard to the pool.
// It checks if the pool for the given host exists, and if so, it calls the Put method on that pool.
// If the pool does not exist, it panics with the given host.
//
// Parameters:
//   - host: The host to which the shard belongs.
//
// Returns:
//   - error: The error that occurred during the put operation.
//
// TODO : unit tests
func (c *cPool) Put(host shard.Shard) error {
	if val, ok := c.pools.Load(host.Instance().Hostname()); ok {
		return val.(Pool).Put(host)
	} else {
		/* very bad */
		panic(host)
	}
}

// Discard removes a shard from the pool.
// If the shard is found in the pool, it calls the Discard method of the corresponding pool.
// If the shard is not found in the pool, it panics with the shard as the argument.
//
// Parameters:
//   - sh: The shard to be removed from the pool.
//
// Returns:
//   - error: The error that occurred during the discard operation.
//
// TODO : unit tests
func (c *cPool) Discard(sh shard.Shard) error {
	if val, ok := c.pools.Load(sh.Instance().Hostname()); ok {
		return val.(Pool).Discard(sh)
	} else {
		/* very bad */
		panic(sh)
	}
}

// InitRule initializes the backend rule for the cPool.
// It takes a pointer to a config.BackendRule as input and sets it as the backend rule for the cPool.
// Returns an error if any.
//
// Parameters:
//   - rule: The backend rule to be set for the cPool.
//
// Returns:
//   - error: The error that occurred during the initialization of the backend rule.
//
// TODO : unit tests
func (c *cPool) InitRule(rule *config.BackendRule) error {
	c.beRule = rule
	return nil
}

var _ MultiShardPool = &cPool{}
