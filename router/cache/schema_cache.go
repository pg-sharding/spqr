package cache

import (
	"fmt"
	"math/rand/v2"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pkg/errors"
)

type SchemaCache struct {
	tableColumnsCache sync.Map
	shardMapping      map[string]*config.Shard
	be                *config.BackendRule

	pool *pool.DBPool
}

func NewSchemaCache(shardMapping map[string]*config.Shard, be *config.BackendRule) *SchemaCache {
	var preferAZ string
	if config.RouterConfig().PreferSameAvailabilityZone {
		preferAZ = config.RouterConfig().AvailabilityZone
	}
	p := pool.NewDBPool(shardMapping, &startup.StartupParams{}, preferAZ)
	p.SetRule(be)

	return &SchemaCache{
		shardMapping:      shardMapping,
		pool:              p,
		be:                be,
		tableColumnsCache: sync.Map{},
	}
}

func (c *SchemaCache) GetColumns(schemaName, tableName string) ([]string, error) {
	c.tableColumnsCache.Range(func(key, value any) bool {
		spqrlog.Zero.Info().Interface("key", key).Interface("value", value).Msg("here")
		return true
	})
	if c.be == nil {
		return nil, errors.Errorf("backend rule was not provided")
	}

	if schemaName == "" {
		schemaName = "public"
	}

	if v, ok := c.tableColumnsCache.Load(fmt.Sprintf("%s.%s", schemaName, tableName)); ok {
		return v.([]string), nil
	}

	shardName := ""
	var host config.Host
	for name, v := range c.shardMapping {
		hosts := v.HostsAZ()
		host = hosts[rand.Int()%len(hosts)]
		shardName = name
		break
	}

	conn, err := c.pool.ConnectionHost(rand.Uint(), kr.ShardKey{Name: shardName, RW: false}, host)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = c.pool.Put(conn)
	}()

	for _, msg := range []pgproto3.FrontendMessage{
		&pgproto3.Parse{Query: `SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2`},
		&pgproto3.Bind{Parameters: [][]byte{[]byte(schemaName), []byte(tableName)}},
		&pgproto3.Execute{},
		&pgproto3.Close{ObjectType: 'S'},
		&pgproto3.Sync{},
	} {
		if err := conn.Send(msg); err != nil {
			return nil, err
		}
	}

	columns := []string{}
	for {
		msg, err := conn.Receive()
		if err != nil {
			return nil, err
		}

		switch v := msg.(type) {
		case *pgproto3.ParseComplete:
		case *pgproto3.BindComplete:
		case *pgproto3.ReadyForQuery:
			c.tableColumnsCache.Store(fmt.Sprintf("%s.%s", schemaName, tableName), columns)
			return columns, nil
		case *pgproto3.DataRow:
			for _, value := range v.Values {
				columns = append(columns, string(value))
			}
		case *pgproto3.CommandComplete:
		case *pgproto3.ErrorResponse:
			return nil, fmt.Errorf("%s", v.Message)
		default:
			continue
		}
	}
}

func (c *SchemaCache) Reset() {
	c.tableColumnsCache.Clear()
}
