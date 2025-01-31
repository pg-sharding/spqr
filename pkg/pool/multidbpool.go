package pool

import (
	"container/list"
	"fmt"
	"math/rand"
	"sync"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

type MultiDBPool struct {
	dbs      sync.Map
	be       *config.BackendRule
	mapping  map[string]*config.Shard
	poolSize int

	queue *list.List
	conns sync.Map
}

func NewMultiDBPool(mapping map[string]*config.Shard, be *config.BackendRule, poolSize int) *MultiDBPool {
	return &MultiDBPool{
		be:       be,
		mapping:  mapping,
		queue:    list.New(),
		poolSize: poolSize,
	}
}

func (p *MultiDBPool) Put(conn shard.Shard) error {
	spqrlog.Zero.Debug().
		Uint("shard", conn.ID()).
		Str("host", conn.Instance().Hostname()).
		Msg("put connection to hostname from multidb pool")

	connElement, ok := p.conns.Load(conn.ID())
	if !ok {
		// already discarded
		return nil
	}

	pool, ok := p.dbs.Load(conn.DB())
	if !ok {
		panic(fmt.Sprintf("db %s not found in pool", conn.DB()))
	}

	if conn.TxStatus() != txstatus.TXIDLE {
		p.queue.Remove(connElement.(*list.Element))
		p.conns.Delete(conn.ID())
	}

	return pool.(*DBPool).Put(conn)
}

func (p *MultiDBPool) Discard(conn shard.Shard) error {
	spqrlog.Zero.Debug().
		Uint("shard", conn.ID()).
		Str("host", conn.Instance().Hostname()).
		Msg("discard connection to hostname from multidb pool")

	connElement, ok := p.conns.Load(conn.ID())
	if !ok {
		// already discarded
		return nil
	}

	pool, ok := p.dbs.Load(conn.DB())
	if !ok {
		panic(fmt.Sprintf("db %s not found in pool", conn.DB()))
	}

	p.queue.Remove(connElement.(*list.Element))
	p.conns.Delete(conn.ID())

	return pool.(*DBPool).Discard(conn)
}

func (p *MultiDBPool) Connection(db string) (shard.Shard, error) {
	var pool *DBPool
	poolElement, exist := p.dbs.Load(db)

	// get or create db pool
	if !exist {
		beRule := *p.be
		beRule.DB = db
		pool = NewDBPool(p.mapping, &startup.StartupParams{}, "")
		pool.SetRule(&beRule)
		p.dbs.Store(db, pool)
	} else {
		pool = poolElement.(*DBPool)
	}

	// get random host from random shard
	shardName, randShard := getRandomShard(p.mapping)
	hosts := randShard.HostsAZ()
	host := hosts[rand.Int()%len(hosts)]

	// get connection
	conn, err := pool.ConnectionHost(uint(rand.Uint64()), kr.ShardKey{Name: shardName, RW: false}, host)
	if err != nil {
		return nil, err
	}

	// LRU
	if el, ok := p.conns.Load(conn.ID()); ok {
		p.queue.MoveToFront(el.(*list.Element))
	} else {
		if p.queue.Len() == p.poolSize {
			back := p.queue.Back()
			if back != nil {
				lruConn := p.queue.Remove(back)
				shardConn := lruConn.(shard.Shard)
				_ = p.Discard(shardConn)
			}
		}
		el := p.queue.PushFront(conn)
		p.conns.Store(conn.ID(), el)
	}

	return conn, nil
}

func (p *MultiDBPool) View() Statistics {
	panic("DBPool.View not implemented")
}

func getRandomShard(mapping map[string]*config.Shard) (string, *config.Shard) {
	i := rand.Int() % len(mapping)
	for k, v := range mapping {
		if i == 0 {
			return k, v
		}
		i--
	}
	return "", nil
}
