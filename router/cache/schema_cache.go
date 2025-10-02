package cache

import (
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/pool"
)

type SchemaCache struct {
	tableColumnsCache sync.Map
	shardMapping      map[string]*config.Shard
	be                *config.BackendRule

	pool *pool.MultiDBPool
}

func NewSchemaCache(shardMapping map[string]*config.Shard, be *config.BackendRule) *SchemaCache {
	poolSize := config.ValueOrDefaultInt(config.RouterConfig().MultiDBPoolSize, 5)
	p := pool.NewMultiDBPool(shardMapping, be, poolSize)

	return &SchemaCache{
		shardMapping:      shardMapping,
		pool:              p,
		be:                be,
		tableColumnsCache: sync.Map{},
	}
}

func (c *SchemaCache) GetColumns(db, schemaName, tableName string) ([]string, error) {
	if c.be == nil {
		return nil, fmt.Errorf("backend rule was not provided")
	}

	if schemaName == "" {
		schemaName = "public"
	}

	if v, ok := c.tableColumnsCache.Load(fmt.Sprintf("%s.%s", schemaName, tableName)); ok {
		return v.([]string), nil
	}

	conn, err := c.pool.Connection(db)
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
