package internal

import (
	"sync"

	"github.com/wal-g/tracelog"
)

type ClientPool interface {
	ClientPoolForeach(cb func(client Client) error) error

	Put(client Client) error
	Pop(client Client) error
}

type ClientPoolImpl struct {
	mu   sync.Mutex
	pool map[string]Client
}

var _ ClientPool = &ClientPoolImpl{}

func (c *ClientPoolImpl) Put(client Client) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pool[client.ID()] = client

	return nil
}

func (c *ClientPoolImpl) Pop(client Client) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.pool, client.ID())

	return nil
}

func (c *ClientPoolImpl) ClientPoolForeach(cb func(client Client) error) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cl := range c.pool {
		if err := cb(cl); err != nil {
			tracelog.ErrorLogger.PrintError(err)
		}
	}

	return nil
}
