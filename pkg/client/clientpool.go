package client

import (
	"sync"

	spqrlog "github.com/pg-sharding/spqr/pkg/spqrlog"
)


type ClientInfoImpl struct {
	Client
}

type Pool interface {
	ClientPoolForeach(cb func(client Client) error) error

	Put(client Client) error
	Pop(id uint) (bool, error)
}

type PoolImpl struct {
	mu   sync.Mutex
	pool map[uint]Client
}

var _ Pool = &PoolImpl{}

// TODO : unit tests

// Put adds a client to the client pool.
//
// It acquires a write lock on the mutex to ensure exclusive access to the pool.
// Then, it adds the provided client to the pool using the client's ID as the key.
// Finally, it releases the lock and returns nil.
//
// Parameters:
//   - client (Client): The client to be added to the pool.
//
// Returns:
//   - error: An error if any occurred during the process.
func (c *PoolImpl) Put(client Client) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pool[client.ID()] = client

	return nil
}

func (c *PoolImpl) ListClients() []Client {
	c.mu.Lock()
	defer c.mu.Unlock()

	var clients []Client
	for _, cl := range c.pool {
		clients = append(clients, cl)
	}

	return clients
}

// TODO : unit tests

// Pop removes a client with the specified ID from the client pool.
//
// It first acquires a write lock on the mutex to ensure exclusive access to the pool.
// Then, it checks if a client with the specified ID exists in the pool.
// If it does, it closes the client, removes it from the pool, and returns true and nil.
// Otherwise, it returns false and nil.
//
// Parameters:
//   - id (uint): The ID of the client to be removed from the pool.
//
// Returns:
//   - ok: A boolean indicating whether the client was successfully removed from the pool.
//   - error: An error if any occurred during the process, including context cancellation or timeout.
func (c *PoolImpl) Pop(id uint) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	cl, ok := c.pool[id]
	if ok {
		err = cl.Close()
		delete(c.pool, id)
	}

	return ok, err
}

// TODO : unit tests

// ClientPoolForeach iterates over all clients in the client pool and executes the provided function for each client.
//
// The provided function should have the following signature:
//
//	func(clientID uint, client Client) error
//
// Parameters:
//   - f (func): The function to be executed for each client.
//
// Returns:
//   - error: An error if any occurred during the iteration.
func (c *PoolImpl) ClientPoolForeach(cb func(client Client) error) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cl := range c.pool {
		if err := cb(ClientInfoImpl{Client: cl}); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}

	return nil
}

// NewClientPool creates a new instance of the PoolImpl struct, which implements the Pool interface.
//
// It initializes the pool map with an empty map and the mutex with a new sync.Mutex.
// The function returns a pointer to the newly created PoolImpl instance.
//
// Parameters:
// - None.
//
// Returns:
// - Pool: A pointer to the newly created PoolImpl instance.
func NewClientPool() Pool {
	return &PoolImpl{
		pool: map[uint]Client{},
		mu:   sync.Mutex{},
	}
}
