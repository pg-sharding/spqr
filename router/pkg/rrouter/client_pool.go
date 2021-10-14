package rrouter

import (
	"sync"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/wal-g/tracelog"
)

type ClientPool interface {
	ClientPoolForeach(cb func(client client.Client) error) error

	Put(client client.Client) error
	Pop(client client.Client) error

	Shutdown() error
}

type ClientPoolImpl struct {
	mu   sync.Mutex
	pool map[string]client.Client
}

var _ ClientPool = &ClientPoolImpl{}

func (c *ClientPoolImpl) Put(client client.Client) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pool[client.ID()] = client

	return nil
}

func (c *ClientPoolImpl) Pop(client client.Client) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.pool, client.ID())

	return nil
}

func (c *ClientPoolImpl) Shutdown() error {

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cl := range c.pool {
		go func(cl client.Client) {
			tracelog.InfoLogger.PrintError(cl.Shutdown())
		}(cl)
	}

	return nil
}
func (c *ClientPoolImpl) ClientPoolForeach(cb func(client client.Client) error) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cl := range c.pool {
		if err := cb(cl); err != nil {
			tracelog.ErrorLogger.PrintError(err)
		}
	}

	return nil
}
func NewClientPool() *ClientPoolImpl {
	return &ClientPoolImpl{
		pool: map[string]client.Client{},
		mu:   sync.Mutex{},
	}
}
