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

type shardHostPool struct {
	mu   sync.Mutex
	pool []shard.ShardHostInstance

	queue chan struct{}

	active map[uint]shard.ShardHostInstance

	alloc ConnectionAllocFn

	beRule *config.BackendRule

	/* dedicated */
	host string
	az   string

	connectionLimit            int
	connectionRetries          int
	connectionRetrySleepSlice  int
	connectionRetryRandomSleep int
}

var _ Pool = &shardHostPool{}

// NewShardHostPool creates a new instance of shardPool based on the provided ConnectionAllocFn, host, and BackendRule.
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
func NewShardHostPool(allocFn ConnectionAllocFn, host config.Host, beRule *config.BackendRule) Pool {
	connLimit := config.ValueOrDefaultInt(beRule.ConnectionLimit, defaultInstanceConnectionLimit)
	connRetries := config.ValueOrDefaultInt(beRule.ConnectionRetries, defaultInstanceConnectionRetries)

	ret := &shardHostPool{
		mu:     sync.Mutex{},
		pool:   nil,
		active: make(map[uint]shard.ShardHostInstance),
		alloc:  allocFn,
		beRule: beRule,
		host:   host.Address,
		az:     host.AZ,

		connectionLimit:            connLimit,
		connectionRetries:          connRetries,
		connectionRetrySleepSlice:  50,
		connectionRetryRandomSleep: 10,
	}

	ret.queue = make(chan struct{}, connLimit)
	for range connLimit {
		ret.queue <- struct{}{}
	}

	spqrlog.Zero.Debug().
		Uint("pool", spqrlog.GetPointer(ret)).
		Int("tokens", connLimit).
		Msg("initialized pool queue with tokens")

	return ret
}

func (h *shardHostPool) View() Statistics {
	h.mu.Lock()
	defer h.mu.Unlock()

	return Statistics{
		DB:                h.beRule.DB,
		Usr:               h.beRule.Usr,
		Hostname:          h.host,
		RouterName:        "unknown",
		UsedConnections:   len(h.active),
		IdleConnections:   len(h.pool),
		QueueResidualSize: len(h.queue),
	}
}

var (
	ConnLimitToken = struct{}{}
)

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
func (h *shardHostPool) Connection(clid uint, shardKey kr.ShardKey) (shard.ShardHostInstance, error) {
	if err := func() error {
		// TODO refactor
		for range h.connectionRetries {
			select {
			// TODO: configure waits using backend rule
			case <-time.After(time.Duration(h.connectionRetrySleepSlice) * time.Millisecond * time.Duration(1+rand.Int31()%int32(h.connectionRetryRandomSleep))):
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

	var sh shard.ShardHostInstance

	/* reuse cached connection, if any */
	for {
		/* try to get non-stale (not invalidated connection) */
		/* TDB: per-bucket lock */
		h.mu.Lock()

		if len(h.pool) > 0 {
			sh, h.pool = h.pool[0], h.pool[1:]

			if sh.IsStale() {
				h.mu.Unlock()
				_ = sh.Close()
				continue
			}

			h.active[sh.ID()] = sh
			h.mu.Unlock()

			spqrlog.Zero.Debug().
				Uint("pool", spqrlog.GetPointer(h)).
				Uint("client", clid).
				Uint("shard", sh.ID()).
				Str("host", sh.Instance().Hostname()).
				Uint("id", sh.ID()).
				Msg("connection pool for client: reuse cached shard connection to instance")
			return sh, nil
		}

		h.mu.Unlock()
		break
	}

	// do not hold lock on poolRW while allocate new connection
	var err error
	sh, err = h.alloc(shardKey, config.Host{Address: h.host, AZ: h.az}, h.beRule)
	if err != nil {
		// return acquired token
		h.queue <- ConnLimitToken
		return nil, err
	}

	spqrlog.Zero.Debug().
		Uint("pool", spqrlog.GetPointer(h)).
		Str("key", shardKey.Name).
		Str("host", h.host).Uint("id", sh.ID()).
		Msg("allocated new connections")

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
func (h *shardHostPool) Discard(sh shard.ShardHostInstance) error {
	spqrlog.Zero.Debug().Uint("pool", spqrlog.GetPointer(h)).
		Uint("shard", sh.ID()).
		Str("host", sh.Instance().Hostname()).
		Msg("discard connection to hostname from pool")

	/* do not hold mutex while cleanup server connection */
	err := sh.Close()

	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.active[sh.ID()]; !ok {
		// double free
		panic(fmt.Sprintf("data corruption: connection already discarded: %v, hostname: %s, shard %s", sh.ID(), sh.InstanceHostname(), sh.ShardKeyName()))
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
func (h *shardHostPool) Put(sh shard.ShardHostInstance) error {
	spqrlog.Zero.Debug().Uint("pool", spqrlog.GetPointer(h)).
		Uint("shard", sh.ID()).
		Str("host", sh.Instance().Hostname()).
		Msg("put connection back to pool")

	if sh.TxStatus() != txstatus.TXIDLE {
		return h.Discard(sh)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.active[sh.ID()]; !ok {
		panic(fmt.Sprintf("data corruption: connection already put: %v, hostname: %s, shard %s", sh.ID(), sh.InstanceHostname(), sh.ShardKeyName()))
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
func (h *shardHostPool) ForEach(cb func(sh shard.ShardHostCtl) error) error {
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

/* pool with many hosts */

type cPool struct {
	PoolIterator
	Pool

	pools sync.Map

	alloc ConnectionAllocFn

	id uint

	beRule *config.BackendRule
}

// ID implements MultiShardPool.
func (c *cPool) ID() uint {
	return c.id
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
func NewPool(allocFn ConnectionAllocFn) ShardHostsPool {
	rt := &cPool{
		pools: sync.Map{},
		alloc: allocFn,
	}

	rt.id = spqrlog.GetPointer(rt)
	return rt
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
func (c *cPool) ForEach(cb func(sh shard.ShardHostCtl) error) error {
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
func (c *cPool) ConnectionHost(clid uint, shardKey kr.ShardKey, host config.Host) (shard.ShardHostInstance, error) {
	var pool Pool
	if val, ok := c.pools.Load(host.Address); !ok {
		pool = NewShardHostPool(c.alloc, host, c.beRule)
		v, _ := c.pools.LoadOrStore(host.Address, pool)

		pool = v.(Pool)
	} else {
		pool = val.(Pool)
	}
	return pool.Connection(clid, shardKey)
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
func (c *cPool) Put(sh shard.ShardHostInstance) error {
	if val, ok := c.pools.Load(sh.Instance().Hostname()); ok {
		return val.(Pool).Put(sh)
	} else {
		panic(fmt.Sprintf("cPool.Put failed, hostname %s not found", sh.Instance().Hostname()))
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
func (c *cPool) Discard(sh shard.ShardHostInstance) error {
	if val, ok := c.pools.Load(sh.Instance().Hostname()); ok {
		return val.(Pool).Discard(sh)
	} else {
		panic(fmt.Sprintf("cPool.Discard failed, hostname %s not found", sh.Instance().Hostname()))
	}
}

// SetRule initializes the backend rule for the cPool.
// It takes a pointer to a config.BackendRule as input and sets it as the backend rule for the cPool.
//
// Parameters:
//   - rule: The backend rule to be set for the cPool.
//
// TODO : unit tests
func (c *cPool) SetRule(rule *config.BackendRule) {
	c.beRule = rule
}

var _ ShardHostsPool = &cPool{}
