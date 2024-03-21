package datatransfers

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"io"
	"os"
	"strings"
	"sync"
)

type MoveTableRes struct {
	TableSchema string `db:"table_schema"`
	TableName   string `db:"table_name"`
}

// TODO: use schema
// var schema = flag.String("schema", "", "")

type ProxyW struct {
	w io.WriteCloser
}

func (p *ProxyW) Write(bt []byte) (int, error) {
	return p.w.Write(bt)
}

var shards *config.DatatransferConnections
var lock sync.RWMutex

var localConfigDir = "/../../cmd/mover/shard_data.yaml"

func createConnString(shardID string) string {
	lock.Lock()
	defer lock.Unlock()

	sd, ok := shards.ShardsData[shardID]
	if !ok {
		return ""
	}
	if len(sd.Hosts) == 0 {
		return ""
	}
	// TODO find_master
	host := strings.Split(sd.Hosts[0], ":")[0]
	port := strings.Split(sd.Hosts[0], ":")[1]
	return fmt.Sprintf("user=%s host=%s port=%s dbname=%s password=%s", sd.User, host, port, sd.DB, sd.Password)
}

func LoadConfig(path string) error {
	var err error
	lock.Lock()
	defer lock.Unlock()

	shards, err = config.LoadShardDataCfg(path)
	if err != nil {
		p, _ := os.Getwd()
		shards, err = config.LoadShardDataCfg(p + localConfigDir)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
MoveKeys performs physical key-range move from one datashard to another.
It is assumed that passed key range is already locked on every online spqr-router.

Steps:
  - create postgres_fdw on receiving shard
  - copy data from sending shard to receiving shard via fdw
  - delete data from sending shard
*/
func MoveKeys(ctx context.Context, fromId, toId string, krg *kr.KeyRange, ds *distributions.Distribution, db qdb.XQDB, cr coordinator.Coordinator) error {
	tx, err := db.GetTransferTx(ctx, krg.ID)
	if err != nil {
		return err
	}
	if tx == nil {
		tx = &qdb.DataTransferTransaction{
			ToShardId:   toId,
			FromShardId: fromId,
			Status:      qdb.Planned,
		}
		if err = db.RecordTransferTx(ctx, krg.ID, tx); err != nil {
			return err
		}
	}
	if shards == nil {
		err := LoadConfig(config.CoordinatorConfig().ShardDataCfg)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error loading config")
		}
	}

	from, err := pgx.Connect(ctx, createConnString(fromId)) // nolint: staticcheck
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error connecting to shard")
		return err
	}
	to, err := pgx.Connect(ctx, createConnString(toId)) // nolint: staticcheck
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error connecting to shard")
		return err
	}

	upperBound, err := resolveNextBound(ctx, krg, cr)
	if err != nil {
		return err
	}

	for tx != nil {
		switch tx.Status {
		case qdb.Planned:
			fromShard := shards.ShardsData[fromId]
			toShard := shards.ShardsData[toId]
			dbName := fromShard.DB
			fromHost := strings.Split(fromShard.Hosts[0], ":")[0]
			serverName := fmt.Sprintf("%s_%s_%s", strings.Split(toShard.Hosts[0], ":")[0], dbName, fromHost)
			// TODO find_master
			_, err = to.Exec(ctx, fmt.Sprintf(`CREATE server IF NOT EXISTS %s FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname '%s', host '%s', port '%s')`, serverName, dbName, fromHost, strings.Split(fromShard.Hosts[0], ":")[1]))
			if err != nil {
				return err
			}
			// TODO check if name is taken
			schemaName := fmt.Sprintf("%s_schema", serverName)
			if _, err = to.Exec(ctx, fmt.Sprintf(`DROP USER MAPPING IF EXISTS FOR %s SERVER %s`, toShard.User, serverName)); err != nil {
				return err
			}
			if _, err = to.Exec(ctx, fmt.Sprintf(`CREATE USER MAPPING FOR %s SERVER %s OPTIONS (user '%s', password '%s')`, toShard.User, serverName, fromShard.User, fromShard.Password)); err != nil {
				return err
			}
			// TODO check if schemaName is not used by relations (needs schemas in distributions)
			if _, err = to.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaName)); err != nil {
				return err
			}
			if _, err = to.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schemaName)); err != nil {
				return err
			}
			_, err = to.Exec(ctx, fmt.Sprintf(`IMPORT FOREIGN SCHEMA public FROM SERVER %s INTO %s`, serverName, schemaName))
			if err != nil {
				return err
			}
			for _, rel := range ds.Relations {
				krCondition := getKRCondition(rel, krg, upperBound)
				// TODO account for schema
				res := from.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) > 0 as table_exists FROM information_schema.tables WHERE table_name = '%s'`, strings.ToLower(rel.Name)))
				fromTableExists := false
				if err = res.Scan(&fromTableExists); err != nil {
					return err
				}
				// TODO test this
				if !fromTableExists {
					continue
				}
				res = from.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s`, rel.Name, krCondition))
				fromCount := 0
				if err = res.Scan(&fromCount); err != nil {
					return err
				}
				res = to.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) > 0 as table_exists FROM information_schema.tables WHERE table_name = '%s'`, strings.ToLower(rel.Name)))
				toTableExists := false
				if err = res.Scan(&toTableExists); err != nil {
					return err
				}
				// TODO test this
				if !toTableExists {
					return fmt.Errorf("relation %s does not exist on receiving shard", rel.Name)
				}
				res = from.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s`, rel.Name, krCondition))
				toCount := 0
				if err = res.Scan(&toCount); err != nil {
					return err
				}
				if toCount == fromCount {
					continue
				}
				if toCount > 0 && fromCount != 0 {
					return fmt.Errorf("key count on sender & receiver mismatch")
				}
				query := fmt.Sprintf(`
					INSERT INTO %s
					SELECT FROM %s
					WHERE %s
`, rel.Name, fmt.Sprintf("%s.%s", schemaName, rel.Name), krCondition)
				_, err = to.Exec(ctx, query)
				if err != nil {
					return err
				}
			}
			tx.Status = qdb.DataCopied
			err = db.RecordTransferTx(ctx, krg.ID, tx)
			if err != nil {
				return err
			}
		case qdb.DataCopied:
			for _, rel := range ds.Relations {
				_, err = from.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s`, rel.Name, getKRCondition(rel, krg, upperBound)))
				if err != nil {
					return err
				}
			}
			if err = db.RemoveTransferTx(ctx, krg.ID); err != nil {
				return err
			}
			tx = nil
		default:
			return fmt.Errorf("incorrect data transfer transaction status: %s", tx.Status)
		}
	}

	return nil
}

func resolveNextBound(ctx context.Context, krg *kr.KeyRange, cr coordinator.Coordinator) (kr.KeyRangeBound, error) {
	krs, err := cr.ListKeyRanges(ctx, krg.Distribution)
	if err != nil {
		return nil, err
	}
	var bound kr.KeyRangeBound
	for _, kRange := range krs {
		if kr.CmpRangesLess(krg.LowerBound, kRange.LowerBound) && (bound == nil || kr.CmpRangesLess(kRange.LowerBound, bound)) {
			bound = kRange.LowerBound
		}
	}
	return bound, nil
}

func getKRCondition(rel *distributions.DistributedRelation, kRange *kr.KeyRange, nextBound kr.KeyRangeBound) string {
	buf := make([]string, len(rel.DistributionKey))
	for i, entry := range rel.DistributionKey {
		// TODO remove after multidimensional key range support
		if i > 0 {
			break
		}
		// TODO add hash (depends on col type)
		hashedCol := entry.Column
		if nextBound != nil {
			buf[i] = fmt.Sprintf("%s >= %s AND %s < %s", hashedCol, string(kRange.LowerBound), hashedCol, string(nextBound))
		} else {
			buf[i] = fmt.Sprintf("%s >= %s", hashedCol, string(kRange.LowerBound))
		}
	}
	return strings.Join(buf, " AND ")
}
