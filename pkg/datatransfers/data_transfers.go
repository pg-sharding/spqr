package datatransfers

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pg-sharding/spqr/coordinator/statistics"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/router/rfqn"

	pgx "github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
)

const spqrguardReferenceRelationLock = 69

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
	spqrlog.Zero.Debug().Msg("called LoadConfig")
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
func MoveKeys(ctx context.Context, fromId, toId string, krg *kr.KeyRange, ds *distributions.Distribution, db qdb.XQDB, mgr meta.EntityMgr) error {
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
	fromCfg, ok := shards.ShardsData[fromId]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "shard with ID \"%s\" not found in config", fromId)
	}
	from, err := GetMasterConnection(ctx, fromCfg)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error connecting to shard")
		return err
	}
	toCfg, ok := shards.ShardsData[toId]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "shard with ID \"%s\" not found in config", toId)
	}
	to, err := GetMasterConnection(ctx, toCfg)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error connecting to shard")
		return err
	}

	upperBound, err := resolveNextBound(ctx, krg, mgr)
	if err != nil {
		return err
	}

	for tx != nil {
		switch tx.Status {
		case qdb.Planned:
			t := time.Now()
			// copy data of key range to receiving shard
			if err = copyData(ctx, from, to, fromId, toId, krg, ds, upperBound); err != nil {
				return err
			}
			tx.Status = qdb.DataCopied
			err = db.RecordTransferTx(ctx, krg.ID, tx)
			statistics.RecordShardOperation("copyData", time.Since(t))
			if err != nil {
				return err
			}
		case qdb.DataCopied:
			// drop data from sending shard
			t := time.Now()
			ftx, err := from.Begin(ctx)
			if err != nil {
				return fmt.Errorf("could not delete data: could not begin transaction: %s", err)
			}
			if _, err := ftx.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
				return fmt.Errorf("could not delete data: error deferring constraints: %s", err)
			}
			if config.CoordinatorConfig().DataMoveDisableTriggers {
				if _, err := ftx.Exec(ctx, "SET session_replication_role = replica"); err != nil {
					return fmt.Errorf("failed to disable triggers: %s", err)
				}
			}
			for _, rel := range ds.Relations {
				res := ftx.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) > 0 as table_exists FROM information_schema.tables WHERE table_name = '%s' AND table_schema = '%s'`, strings.ToLower(rel.Name), rel.GetSchema()))
				fromTableExists := false
				if err = res.Scan(&fromTableExists); err != nil {
					return err
				}
				if !fromTableExists {
					continue
				}
				cond, err := kr.GetKRCondition(rel, krg, upperBound, "")
				if err != nil {
					return err
				}
				_, err = ftx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s`, rel.GetFullName(), cond))
				if err != nil {
					return fmt.Errorf("could not delete data: error executing DELETE FROM: %s", err)
				}
			}
			if err = ftx.Commit(ctx); err != nil {
				return fmt.Errorf("could not delete data: could not commit transaction: %s", err)
			}
			statistics.RecordShardOperation("dropData", time.Since(t))
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

func SyncReferenceRelation(ctx context.Context, fromId, toId string, rel *rrelation.ReferenceRelation, db qdb.XQDB) error {
	tx, err := db.GetTransferTx(ctx, rel.TableName)
	if err != nil {
		return err
	}

	transferKey := rel.GetFullName()

	if tx == nil {
		// No transaction in progress
		tx = &qdb.DataTransferTransaction{
			ToShardId:   toId,
			FromShardId: fromId,
			Status:      qdb.Planned,
		}
		if err = db.RecordTransferTx(ctx, transferKey, tx); err != nil {
			return err
		}
	}
	if shards == nil {
		err := LoadConfig(config.CoordinatorConfig().ShardDataCfg)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error loading config")
		}
	}
	fromCfg, ok := shards.ShardsData[fromId]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "shard with ID \"%s\" not found in config", fromId)
	}
	from, err := GetMasterConnection(ctx, fromCfg)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error connecting to shard")
		return err
	}
	toCfg, ok := shards.ShardsData[toId]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "shard with ID \"%s\" not found in config", toId)
	}
	to, err := GetMasterConnection(ctx, toCfg)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error connecting to shard")
		return err
	}

	for tx != nil {
		switch tx.Status {
		case qdb.Planned:
			// lock reference relation on its current shards
			if err = lockReferenceRelation(ctx, rel); err != nil {
				return err
			}
			tx.Status = qdb.Locked
			err = db.RecordTransferTx(ctx, transferKey, tx)
			if err != nil {
				return err
			}
		case qdb.Locked:
			// copy data of key range to receiving shard
			if err = copyReferenceRelationData(ctx, from, to, fromId, toId, rel); err != nil {
				return err
			}
			tx.Status = qdb.DataCopied
			err = db.RecordTransferTx(ctx, transferKey, tx)
			if err != nil {
				return err
			}
		case qdb.DataCopied:
			// unlock reference relation
			if err = unlockReferenceRelation(ctx, rel); err != nil {
				return err
			}
			if err = db.RemoveTransferTx(ctx, transferKey); err != nil {
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
func resolveNextBound(ctx context.Context, krg *kr.KeyRange, cr meta.EntityMgr) (kr.KeyRangeBound, error) {
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

func SetupFDW(ctx context.Context, from, to *pgx.Conn, fromShardId, toShardId string, schemas map[string]struct{}) error {
	if shards == nil {
		err := LoadConfig(config.CoordinatorConfig().ShardDataCfg)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error loading config")
			return err
		}
	}

	fromShard := shards.ShardsData[fromShardId]
	toShard := shards.ShardsData[toShardId]
	dbName := fromShard.DB
	fromHost := strings.Split(fromShard.Hosts[0], ":")[0]
	serverName := fmt.Sprintf("%s_%s_%s", strings.Split(toShard.Hosts[0], ":")[0], dbName, fromHost)
	// create postgres_fdw server on receiving shard
	_, err := to.Exec(ctx, fmt.Sprintf(`CREATE server IF NOT EXISTS %q FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname '%s', host '%s', port '%s')`, serverName, dbName, fromHost, strings.Split(fromShard.Hosts[0], ":")[1]))
	if err != nil {
		return err
	}
	// create user mapping for postgres_fdw server
	// TODO check if name is taken
	if _, err = to.Exec(ctx, fmt.Sprintf(`DROP USER MAPPING IF EXISTS FOR %s SERVER %q`, toShard.User, serverName)); err != nil {
		return err
	}
	if _, err = to.Exec(ctx, fmt.Sprintf(`CREATE USER MAPPING FOR %s SERVER %q OPTIONS (user '%s', password '%s')`, toShard.User, serverName, fromShard.User, fromShard.Password)); err != nil {
		return err
	}
	// create foreign tables corresponding to such on sending shard
	// TODO check if schemaName is not used by relations (needs schemas in distributions)
	schemaName := fmt.Sprintf("%s_schema", serverName)
	if _, err = to.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schemaName)); err != nil {
		return err
	}
	if _, err = to.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, schemaName)); err != nil {
		return err
	}
	for schema := range schemas {
		if _, err = to.Exec(ctx, fmt.Sprintf(`IMPORT FOREIGN SCHEMA %s FROM SERVER %q INTO %q`, schema, serverName, schemaName)); err != nil {
			return err
		}
	}
	return err
}

func lockReferenceRelation(ctx context.Context, relation *rrelation.ReferenceRelation) error {
	for _, shard := range relation.ShardIds {
		connInfo, ok := shards.ShardsData[shard]
		if !ok {
			return fmt.Errorf("no connection info for shard \"%s\", relation \"%s\"", shard, relation.GetFullName())
		}
		shardConn, err := GetMasterConnection(ctx, connInfo)
		if err != nil {
			return fmt.Errorf("can't lock relation \"%s\": %s", relation.GetFullName(), err)
		}
		if err = lockReferenceRelationOnShard(ctx, shardConn, relation.QualifiedName()); err != nil {
			return fmt.Errorf("can't lock relation \"%s\": %s", relation.GetFullName(), err)
		}
	}
	return nil
}

func lockReferenceRelationOnShard(ctx context.Context, shardConn *pgx.Conn, relation rfqn.RelationFQN) error {
	tx, err := shardConn.Begin(ctx)
	if err != nil {
		return err
	}
	row := tx.QueryRow(ctx, "SELECT value as references_locked FROM spqr_metadata.spqr_global_settings WHERE name = $1", spqrguardReferenceRelationLock)
	val := ""
	if err = row.Scan(&val); err != nil && err != pgx.ErrNoRows {
		return err
	}
	// TODO: process differently to avoid deadlocks
	switch val {
	case "on", "yes", "ok", "true":
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "reference relations already locked", relation.String())
	}
	if _, err = tx.Exec(ctx, "SELECT spqr_metadata.mark_reference_relation($1);", relation.String()); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO spqr_metadata.spqr_global_settings (name, value) VALUES ($1, 'true')", spqrguardReferenceRelationLock); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func unlockReferenceRelation(ctx context.Context, relation *rrelation.ReferenceRelation) error {
	for _, shard := range relation.ShardIds {
		connInfo, ok := shards.ShardsData[shard]
		if !ok {
			return fmt.Errorf("no connection info for shard \"%s\", relation \"%s\"", shard, relation.GetFullName())
		}
		shardConn, err := GetMasterConnection(ctx, connInfo)
		if err != nil {
			return fmt.Errorf("can't lock relation \"%s\": %s", relation.GetFullName(), err)
		}
		if err = unlockReferenceRelationOnShard(ctx, shardConn, relation.QualifiedName()); err != nil {
			return fmt.Errorf("can't lock relation \"%s\": %s", relation.GetFullName(), err)
		}
	}
	return nil
}

func unlockReferenceRelationOnShard(ctx context.Context, shardConn *pgx.Conn, relation rfqn.RelationFQN) error {
	tx, err := shardConn.Begin(ctx)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "DELETE FROM spqr_metadata.spqr_reference_relations WHERE reloid = ($1)::regclass::oid;", relation.String()); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "DELETE FROM spqr_metadata.spqr_global_settings WHERE name=$1", spqrguardReferenceRelationLock); err != nil {
		return err
	}
	return tx.Commit(ctx)
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
func copyData(ctx context.Context, from, to *pgx.Conn, fromShardId, toShardId string, krg *kr.KeyRange, ds *distributions.Distribution, upperBound kr.KeyRangeBound) error {
	schemas := make(map[string]struct{})
	for _, rel := range ds.Relations {
		schemas[rel.GetSchema()] = struct{}{}
	}
	if err := SetupFDW(ctx, from, to, fromShardId, toShardId, schemas); err != nil {
		return err
	}
	fromShard := shards.ShardsData[fromShardId]
	toShard := shards.ShardsData[toShardId]
	dbName := fromShard.DB
	fromHost := strings.Split(fromShard.Hosts[0], ":")[0]
	schemaName := fmt.Sprintf("%s_%s_%s_schema", strings.Split(toShard.Hosts[0], ":")[0], dbName, fromHost)
	tx, err := to.Begin(ctx)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "could not move the data: could not start transaction: %s", err)
	}
	if _, err := tx.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "could not move the data: error deferring contraints: %s", err)
	}
	if config.CoordinatorConfig().DataMoveDisableTriggers {
		if _, err := tx.Exec(ctx, "SET session_replication_role = replica"); err != nil {
			return fmt.Errorf("failed to disable triggers: %s", err)
		}
	}
	for _, rel := range ds.Relations {
		krCondition, err := kr.GetKRCondition(rel, krg, upperBound, "")
		if err != nil {
			return err
		}
		// check that relation exists on sending shard and there is data to copy. If not, skip the relation
		relSchemaName := rel.GetSchema()
		fromTableExists, err := CheckTableExists(ctx, from, strings.ToLower(rel.Name), relSchemaName)
		if err != nil {
			return err
		}
		if !fromTableExists {
			continue
		}
		relFullName := rel.GetFullName()
		fromCount, err := getEntriesCount(ctx, from, relFullName, krCondition)
		if err != nil {
			return err
		}
		// check that relation exists on receiving shard. If not, exit
		toTableExists, err := CheckTableExists(ctx, tx, strings.ToLower(rel.Name), relSchemaName)
		if err != nil {
			return err
		}
		if !toTableExists {
			return fmt.Errorf("relation %s does not exist on receiving shard", rel.Name)
		}
		toCount, err := getEntriesCount(ctx, tx, relFullName, krCondition)
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
		cols, err := getTableColumns(ctx, tx, rfqn.RelationFQN{RelationName: strings.ToLower(rel.Name), SchemaName: rel.GetSchema()})
		if err != nil {
			return err
		}
		colNames := strings.Join(cols, ", ")
		query := fmt.Sprintf(`
					INSERT INTO %s (%s)
					SELECT %s FROM %s
					WHERE %s
`, relFullName, colNames, colNames, fmt.Sprintf("%q.%q", schemaName, strings.ToLower(rel.Name)), krCondition)
		_, err = tx.Exec(ctx, query)
		if err != nil {
			return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "could not move the data: %s", err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "could not move the data: could not execute transaction: %s", err)
	}
	return nil
}

func copyReferenceRelationData(ctx context.Context, from, to *pgx.Conn, fromId, toId string, rel *rrelation.ReferenceRelation) error {

	schemas := make(map[string]struct{})
	schemas[rel.GetSchema()] = struct{}{}

	if err := SetupFDW(ctx, from, to, fromId, toId, schemas); err != nil {
		return err
	}
	fromShard := shards.ShardsData[fromId]
	toShard := shards.ShardsData[toId]
	dbName := fromShard.DB
	fromHost := strings.Split(fromShard.Hosts[0], ":")[0]
	schemaName := fmt.Sprintf("%s_%s_%s_schema", strings.Split(toShard.Hosts[0], ":")[0], dbName, fromHost)

	/* TODO: lock reference relation in spqrguard everywhere */

	// check that relation exists on sending shard and there is data to copy. If not, skip the relation
	// TODO get actual schema
	relSchemaName := rel.GetSchema()
	fromTableExists, err := CheckTableExists(ctx, from, strings.ToLower(rel.TableName), relSchemaName)
	if err != nil {
		return err
	}
	if !fromTableExists {
		return nil
	}
	relFullName := rel.GetFullName()
	fromCount, err := getEntriesCount(ctx, from, relFullName, "true")
	if err != nil {
		return err
	}
	// check that relation exists on receiving shard. If not, exit
	toTableExists, err := CheckTableExists(ctx, to, strings.ToLower(rel.TableName), relSchemaName)
	if err != nil {
		return err
	}
	if !toTableExists {
		return fmt.Errorf("relation %s does not exist on receiving shard", rel.TableName)
	}
	toCount, err := getEntriesCount(ctx, to, relFullName, "true")
	if err != nil {
		return err
	}
	// if data is already copied, skip
	if toCount == fromCount {
		return nil
	}
	// if data is inconsistent, fail
	if toCount > 0 && fromCount != 0 {
		return fmt.Errorf("key count on sender & receiver mismatch")
	}
	tx, err := to.Begin(ctx)
	if err != nil {
		return fmt.Errorf("could not start transaction to copy reference table data: %s", err)
	}

	cols, err := getTableColumns(ctx, tx, rfqn.RelationFQN{RelationName: strings.ToLower(rel.TableName), SchemaName: rel.GetSchema()})
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`
					INSERT INTO %s (%s)
					SELECT %s FROM %s
`, relFullName, strings.Join(cols, ", "), strings.Join(cols, ", "), fmt.Sprintf("%q.%q", schemaName, strings.ToLower(rel.TableName)))
	_, err = tx.Exec(ctx, query)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "could not move the data: %s", err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("could not commit transaction: %s", err)
	}

	return nil
}

func getTableColumns(ctx context.Context, db Queryable, rfqn rfqn.RelationFQN) ([]string, error) {
	cols := make([]string, 0)
	colRows, err := db.Query(ctx, "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2", rfqn.SchemaName, strings.ToLower(rfqn.RelationName))
	if err != nil {
		return nil, fmt.Errorf("failed to get columns of table \"%s\": %s", rfqn.String(), err)
	}
	for colRows.Next() {
		colName := ""
		if err := colRows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("error scanning column name: %s", err)
		}
		cols = append(cols, colName)
	}
	return cols, nil
}

type Queryable interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// CheckTableExists checks if a table exists in the database.
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
func CheckTableExists(ctx context.Context, tx Queryable, relName, schema string) (bool, error) {
	res := tx.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) > 0 as table_exists FROM information_schema.tables WHERE table_name = '%s' AND table_schema = '%s'`, relName, schema))
	exists := false
	if err := res.Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// CheckColumnExists checks if specified column exists in a relation.
//
// Parameters:
// - ctx (context.Context): the context for database operations;
// - conn (*pgx.Conn): the connection to the database;
// - relName (string): the name of the table to check;
// - schema (string): the schema of the table to check;
// - colName (string): the name of the column to check.
//
// Returns:
// - bool: true if the column exists, false otherwise;
// - error: an error if there was a problem executing the query.
func CheckColumnExists(ctx context.Context, conn *pgx.Conn, relName, schema, colName string) (bool, error) {
	res := conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) > 0 as column_exists FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' AND column_name = '%s'`, relName, schema, colName))
	exists := false
	if err := res.Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// CheckConstraints checks for non-deferrable contsraints or constraints referencing external relations.
//
// Parameters:
// - ctx (context.Context): the context for database operations;
// - conn (*pgx.Conn): the connection to the database;
// - dsRels (string): the list of distributed relations, for which to check for constraints;
// - rpRels (string): the list of replicated relations, which can also be referred to by distributed relations;
//
// Returns:
// - bool: true if no such constraints are found, false otherwise;
// - string: name of a constraint, if found;
// - error: an error if there was a problem executing the query.
func CheckConstraints(ctx context.Context, conn *pgx.Conn, dsRels []string, rpRels []string) (bool, string, error) {
	if len(dsRels) == 0 {
		return true, "", nil
	}
	dsRelOidList := make([]string, len(dsRels))
	for i, relName := range dsRels {
		dsRelOidList[i] = fmt.Sprintf("'%s'::regclass::oid", relName)
	}
	dsRelOids := strings.Join(dsRelOidList, ", ")
	rpRelsClause := ""
	if len(rpRels) > 0 {
		refRelOidList := make([]string, len(rpRels))
		for i, relName := range rpRels {
			refRelOidList[i] = fmt.Sprintf("'%s'::regclass::oid", relName)
		}
		rpRelOids := strings.Join(refRelOidList, ", ")
		rpRelsClause = fmt.Sprintf(" and not (confrelid IN (%s))", rpRelOids)
	}
	rows, err := conn.Query(ctx, fmt.Sprintf(`SELECT conname FROM pg_constraint WHERE conrelid IN (%s) and confrelid != 0 and (condeferrable=false or not (confrelid IN (%s)))%s LIMIT 1`, dsRelOids, dsRelOids, rpRelsClause))
	if err != nil {
		return false, "", err
	}
	if !rows.Next() {
		return true, "", nil
	}
	var conName string
	if err = rows.Scan(&conName); err != nil {
		return false, "", err
	}
	return false, conName, nil
}

func CheckHashExtension(ctx context.Context, conn *pgx.Conn) (bool, error) {
	res := conn.QueryRow(ctx, "SELECT count(*) FROM pg_extension WHERE extname = 'spqrhash'")
	count := 0
	if err := res.Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
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
func getEntriesCount(ctx context.Context, conn Queryable, relName string, condition string) (int, error) {
	res := conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s`, relName, condition))
	count := 0
	if err := res.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
