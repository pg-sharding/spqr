package client

import (
	"sync"

	spqrlog "github.com/pg-sharding/spqr/pkg/spqrlog"
)

type RouterClient interface {
	Client

	RAddr() string
}

type RouterClientImpl struct {
	Client
	rAddr string
}

func (rci RouterClientImpl) RAddr() string {
	return rci.rAddr
}

type Pool interface {
	ClientPoolForeach(cb func(client RouterClient) error) error

	Put(client Client) error
	Pop(id string) (bool, error)

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

func (c *PoolImpl) Pop(id string) (bool, error) {
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

func (c *PoolImpl) Shutdown() error {

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cl := range c.pool {
		go func(cl Client) {
			if err := cl.Shutdown(); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
			}
		}(cl)
	}

	return nil
}
func (c *PoolImpl) ClientPoolForeach(cb func(client RouterClient) error) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cl := range c.pool {
		if err := cb(RouterClientImpl{Client: cl, rAddr: "local"}); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}

	return nil
}

func NewClientPool() Pool {
	return &PoolImpl{
		pool: map[string]Client{},
		mu:   sync.Mutex{},
	}
}
