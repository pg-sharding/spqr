package cache

import (
	"fmt"
	"math/rand/v2"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/startup"
)

type SchemaCache struct {
	tableColumnsCache sync.Map
	shardMapping      map[string]*config.Shard

	pool *pool.DBPool
}

func NewSchemaCache(shardMapping map[string]*config.Shard) *SchemaCache {
	var preferAZ string
	if config.RouterConfig().PreferSameAvailabilityZone {
		preferAZ = config.RouterConfig().AvailabilityZone
	}
	return &SchemaCache{
		shardMapping:      shardMapping,
		pool:              pool.NewDBPool(shardMapping, &startup.StartupParams{}, preferAZ),
		tableColumnsCache: sync.Map{},
	}
}

func (c *SchemaCache) GetColumns(be *config.BackendRule, schemaName, tableName string) ([]string, error) {
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

	c.pool.SetRule(be)
	conn, err := c.pool.ConnectionHost(rand.Uint(), kr.ShardKey{Name: shardName, RW: false}, host)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = c.pool.Discard(conn)
	}()

	for _, msg := range []pgproto3.FrontendMessage{
		&pgproto3.Sync{},
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
	rfqCount := 0
	for rfqCount < 2 {
		msg, err := conn.Receive()
		if err != nil {
			return nil, err
		}

		switch v := msg.(type) {
		case *pgproto3.ParseComplete:
		case *pgproto3.BindComplete:
		case *pgproto3.ReadyForQuery:
			rfqCount++
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

	c.tableColumnsCache.Store(fmt.Sprintf("%s.%s", schemaName, tableName), columns)

	return columns, nil
}
