package datatransfers

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/pg-sharding/spqr/pkg/models/distributions"

	pgx "github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
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

// Write writes the given byte slice to the underlying io.WriteCloser.
//
// Parameters:
// - bt ([]byte): a byte slice to be written.
//
// Returns:
// - int: the number of bytes written.
// - error: an error, if any, that occurred during the
func (p *ProxyW) Write(bt []byte) (int, error) {
	return p.w.Write(bt)
}

var shards *config.DatatransferConnections
var lock sync.RWMutex

var localConfigDir = "/../../cmd/mover/shard_data.yaml"

// createConnString generates a connection string for the specified shard ID.
//
// Parameters:
// - shardID (string): The ID of the shard for which the connection string is generated.
//
// Returns:
// - string: The generated connection string. If the shard ID is not found or if there are no hosts for the shard, an empty string is returned.
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

// LoadConfig loads the configuration from the specified path or the localConfigDir directory.
//
// Parameters:
// - path (string): the path to the configuration file.
//
// Returns:
// - error: an error if the configuration cannot be loaded.
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

// MoveKeys performs physical key-range move from one datashard to another.
//
// It is assumed that the passed key range is already locked on every online spqr-router.
// The function performs the following steps:
//   - Create a postgres_fdw on the receiving shard.
//   - Copy data from the sending shard to the receiving shard via fdw.
//   - Delete data from the sending shard.
//
// Parameters:
//   - ctx (context.Context): The context for the function.
//   - fromId (string): the ID of the sending shard.
//   - toId (string): the ID of the receiving shard.
//   - krg (*kr.KeyRange): the KeyRange object representing the key range being moved.
//   - ds (*distributions.Distribution): the Distributions object representing the distribution of data.
//   - db (qdb.XQDB): the XQDB object for interacting with the database.
//   - cr (coordinator.Coordinator): the Coordinator object for coordinating the move.
//
// Returns:
//   - error: an error if the move fails.
func MoveKeys(ctx context.Context, fromId, toId string, krg *kr.KeyRange, ds *distributions.Distribution, db qdb.XQDB, cr coordinator.Coordinator) error {
	tx, err := db.GetTransferTx(ctx, krg.ID)
	if err != nil {
		return err
	}
	if tx == nil {
		// No transaction in progress
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

	from, err := pgx.Connect(ctx, createConnString(fromId))
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error connecting to shard")
		return err
	}
	to, err := pgx.Connect(ctx, createConnString(toId))
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
			// copy data of key range to receiving shard
			if err = copyData(ctx, from, to, fromId, toId, krg, ds, upperBound); err != nil {
				return err
			}
			tx.Status = qdb.DataCopied
			err = db.RecordTransferTx(ctx, krg.ID, tx)
			if err != nil {
				return err
			}
		case qdb.DataCopied:
			// drop data from sending shard
			for _, rel := range ds.Relations {
				// TODO get actual schema
				res := from.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) > 0 as table_exists FROM information_schema.tables WHERE table_name = '%s' AND table_schema = 'public'`, strings.ToLower(rel.Name)))
				fromTableExists := false
				if err = res.Scan(&fromTableExists); err != nil {
					return err
				}
				if !fromTableExists {
					continue
				}
				_, err = from.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s`, strings.ToLower(rel.Name), kr.GetKRCondition(ds, rel, krg, upperBound, "")))
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

// resolveNextBound finds the next lower bound key range from the given key range list that is greater than the lower bound of the given key range.
//
// Parameters:
// - ctx (context.Context): The context for the function.
// - krg (*kr.KeyRange): the key range for which the next lower bound is to be found.
// - cr (coordinator.Coordinator): the coordinator.Coordinator object used to list key ranges.
//
// Returns:
// - kr.KeyRangeBound: the next lower bound key range found, or nil if no such key range exists.
// - error: an error if the key range list cannot be retrieved or if there is an error in the function execution.
func resolveNextBound(ctx context.Context, krg *kr.KeyRange, cr coordinator.Coordinator) (kr.KeyRangeBound, error) {
	krs, err := cr.ListKeyRanges(ctx, krg.Distribution)
	if err != nil {
		return nil, err
	}
	ds, err := cr.GetDistribution(ctx, krg.Distribution)
	if err != nil {
		return nil, err
	}
	var bound kr.KeyRangeBound
	for _, kRange := range krs {
		if kr.CmpRangesLess(krg.LowerBound, kRange.LowerBound, ds.ColTypes) && (bound == nil || kr.CmpRangesLess(kRange.LowerBound, bound, ds.ColTypes)) {
			bound = kRange.LowerBound
		}
	}
	return bound, nil
}

// copyData performs physical key-range move from one datashard to another.
//
// It is assumed that the passed key range is already locked on every online spqr-router.
// The function performs the following steps:
//   - Create a postgres_fdw on the receiving shard.
//   - Copy data from the sending shard to the receiving shard via fdw.
//
// Parameters:
// - ctx (context.Context): The context for the function.
// - from, to (*pgx.Conn): The connections to the sending and receiving shards.
// - fromId, toId (string): the IDs of the sending and receiving shards.
// - krg (*kr.KeyRange): the KeyRange object representing the key range being moved.
// - ds (*distributions.Distribution): the Distributions object representing the distribution of data.
// - upperBound (kr.KeyRangeBound): the upper bound of the key range being moved.
//
// Returns:
// - error: an error if the move fails.
func copyData(ctx context.Context, from, to *pgx.Conn, fromId, toId string, krg *kr.KeyRange, ds *distributions.Distribution, upperBound kr.KeyRangeBound) error {
	fromShard := shards.ShardsData[fromId]
	toShard := shards.ShardsData[toId]
	dbName := fromShard.DB
	fromHost := strings.Split(fromShard.Hosts[0], ":")[0]
	serverName := fmt.Sprintf("%s_%s_%s", strings.Split(toShard.Hosts[0], ":")[0], dbName, fromHost)
	// create postgres_fdw server on receiving shard
	// TODO find master
	_, err := to.Exec(ctx, fmt.Sprintf(`CREATE server IF NOT EXISTS %s FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname '%s', host '%s', port '%s')`, serverName, dbName, fromHost, strings.Split(fromShard.Hosts[0], ":")[1]))
	if err != nil {
		return err
	}
	// create user mapping for postgres_fdw server
	// TODO check if name is taken
	schemaName := fmt.Sprintf("%s_schema", serverName)
	if _, err = to.Exec(ctx, fmt.Sprintf(`DROP USER MAPPING IF EXISTS FOR %s SERVER %s`, toShard.User, serverName)); err != nil {
		return err
	}
	if _, err = to.Exec(ctx, fmt.Sprintf(`CREATE USER MAPPING FOR %s SERVER %s OPTIONS (user '%s', password '%s')`, toShard.User, serverName, fromShard.User, fromShard.Password)); err != nil {
		return err
	}
	// create foreign tables corresponding to such on sending shard
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
		krCondition := kr.GetKRCondition(ds, rel, krg, upperBound, "")
		// check that relation exists on sending shard and there is data to copy. If not, skip the relation
		// TODO get actual schema
		fromTableExists, err := checkTableExists(ctx, from, strings.ToLower(rel.Name), "public")
		if err != nil {
			return err
		}
		if !fromTableExists {
			continue
		}
		fromCount, err := getEntriesCount(ctx, from, rel.Name, krCondition)
		if err != nil {
			return err
		}
		// check that relation exists on receiving shard. If not, exit
		toTableExists, err := checkTableExists(ctx, to, strings.ToLower(rel.Name), "public")
		if err != nil {
			return err
		}
		if !toTableExists {
			return fmt.Errorf("relation %s does not exist on receiving shard", rel.Name)
		}
		toCount, err := getEntriesCount(ctx, to, rel.Name, krCondition)
		if err != nil {
			return err
		}
		// if data is already copied, skip
		if toCount == fromCount {
			continue
		}
		// if data is inconsistent, fail
		if toCount > 0 && fromCount != 0 {
			return fmt.Errorf("key count on sender & receiver mismatch")
		}
		query := fmt.Sprintf(`
					INSERT INTO %s
					SELECT * FROM %s
					WHERE %s
`, strings.ToLower(rel.Name), fmt.Sprintf("%s.%s", schemaName, strings.ToLower(rel.Name)), krCondition)
		_, err = to.Exec(ctx, query)
		if err != nil {
			return err
		}
	}
	return nil
}

// checkTableExists checks if a table exists in the database.
//
// Parameters:
// - ctx (context.Context): The context for the function.
// - conn (*pgx.Conn): the database connection.
// - relName (string): the name of the table to check.
// - schema (string): the schema of the table to check.
//
// Returns:
// - bool: true if the table exists, false otherwise.
// - error: an error if there was a problem executing the query.
func checkTableExists(ctx context.Context, conn *pgx.Conn, relName, schema string) (bool, error) {
	res := conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) > 0 as table_exists FROM information_schema.tables WHERE table_name = '%s' AND table_schema = '%s'`, relName, schema))
	exists := false
	if err := res.Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// getEntriesCount retrieves the number of entries from a database table based on the provided condition.
//
// Parameters:
// - ctx (context.Context): The context for the function.
// - conn (*pgx.Conn): the database connection.
// - relName (string): the name of the table to query.
// - condition (string): the condition to apply in the query.
//
// Returns:
// - int: the count of entries in the table.
// - error: an error if there was a problem executing the query.
func getEntriesCount(ctx context.Context, conn *pgx.Conn, relName string, condition string) (int, error) {
	res := conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s`, relName, condition))
	count := 0
	if err := res.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
