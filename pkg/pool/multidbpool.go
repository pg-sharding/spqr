package pool

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type MultiDBPool struct {
	dbs      sync.Map
	be       *config.BackendRule
	mapping  map[string]*config.Shard
	poolSize int

	// TODO implement LRU cache
}

func NewMultiDBPool(mapping map[string]*config.Shard, be *config.BackendRule, poolSize int) *MultiDBPool {
	return &MultiDBPool{
		be:       be,
		mapping:  mapping,
		poolSize: poolSize,
	}
}

func (p *MultiDBPool) Put(conn shard.ShardHostInstance) error {
	spqrlog.Zero.Debug().
		Uint("shard", conn.ID()).
		Str("host", conn.Instance().Hostname()).
		Msg("put connection to hostname from multidb pool")

	pool, ok := p.dbs.Load(conn.DB())
	if !ok {
		panic(fmt.Sprintf("db %s not found in pool", conn.DB()))
	}

	return pool.(*DBPool).Put(conn)
}

func (p *MultiDBPool) Discard(conn shard.ShardHostInstance) error {
	spqrlog.Zero.Debug().
		Uint("shard", conn.ID()).
		Str("host", conn.Instance().Hostname()).
		Msg("discard connection to hostname from multidb pool")

	pool, ok := p.dbs.Load(conn.DB())
	if !ok {
		panic(fmt.Sprintf("db %s not found in pool", conn.DB()))
	}

	return pool.(*DBPool).Discard(conn)
}

func (p *MultiDBPool) Connection(db string) (shard.ShardHostInstance, error) {
	var pool ShardHostsPool
	poolElement, exist := p.dbs.Load(db)

	// get or create db pool
	if !exist {
		beRule := *p.be
		beRule.DB = db

		/* Create a new DBPool with the given mapping and backend rule
		 * PreferAZ is set to "", so it will not prefer any AZ
		 * DisableCheckInterval because we do not want to check the health of the connection
		 */

		newPool := NewDBPoolWithDisabledFeatures(p.mapping)
		newPool.SetRule(&beRule)
		p.dbs.Store(db, newPool)
		pool = newPool
	} else {
		pool = poolElement.(*DBPool)
	}

	// get random host from random shard
	shardName, randShard := getRandomShard(p.mapping)
	hosts := randShard.HostsAZ()
	host := hosts[rand.Int()%len(hosts)]

	// get connection
	conn, err := pool.ConnectionHost(uint(rand.Uint64()), kr.ShardKey{Name: shardName, RO: false}, host)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (p *MultiDBPool) View() Statistics {
	panic("MultiDBPool.View not implemented")
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
