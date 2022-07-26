package client

import (
	"sync"

	spqrlog "github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/wal-g/tracelog"
)

type Pool interface {
	ClientPoolForeach(cb func(client Client) error) error

	Put(client Client) error
	Pop(client Client) error

	Shutdown() error
}

type PoolImpl struct {
	mu   sync.Mutex
	pool map[string]Client
}

var _ Pool = &PoolImpl{}

func (c *PoolImpl) Put(client Client) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pool[client.ID()] = client

	return nil
}

func (c *PoolImpl) Pop(client Client) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.pool, client.ID())

	return nil
}

func (c *PoolImpl) Shutdown() error {

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cl := range c.pool {
		go func(cl Client) {
			tracelog.InfoLogger.PrintError(cl.Shutdown())
		}(cl)
	}

	return nil
}
func (c *PoolImpl) ClientPoolForeach(cb func(client Client) error) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cl := range c.pool {
		if err := cb(cl); err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}

	return nil
}

func NewClientPool() *PoolImpl {
	return &PoolImpl{
		pool: map[string]Client{},
		mu:   sync.Mutex{},
	}
}
