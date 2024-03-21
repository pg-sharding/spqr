package datatransfers

import (
	"context"
	"fmt"
	pgx "github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
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

type pgxConnIface interface {
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Close(context.Context) error
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
func MoveKeys(ctx context.Context, fromId, toId string, krg *kr.KeyRange, ds *distributions.Distribution, db qdb.XQDB) error {
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

	_, err = pgx.Connect(ctx, createConnString(fromId))
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error connecting to shard")
		return err
	}
	to, err := pgx.Connect(ctx, createConnString(toId))
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error connecting to shard")
		return err
	}

	// bound of the next key range
	// TODO get actual value
	var upperBound kr.KeyRangeBound = make([]byte, 0)

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
			// TODO schema information in distributions
			_, err = to.Exec(ctx, `IMPORT FOREIGN SCHEMA public FROM SERVER $1 INTO $2`, serverName, schemaName)
			if err != nil {
				return err
			}
			for _, rel := range ds.Relations {
				// TODO check range on receiver
				query := fmt.Sprintf(`
					INSERT INTO $1
					SELECT FROM $2
					WHERE %s
`, getKRCondition(krg, upperBound))
				_, err = to.Exec(ctx, query, rel, fmt.Sprintf("%s.%s", schemaName, rel.Name))
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
				_, err = to.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s`, rel.Name, getKRCondition(krg, upperBound)))
			}
			if err = db.RemoveTransferTx(ctx, krg.ID); err != nil {
				return err
			}
		default:
			return fmt.Errorf("incorrect data transfer transaction status: %s", tx.Status)
		}
	}

	return nil
}

// TODO mb separate it to kr package
func getKRCondition(krg *kr.KeyRange, next kr.KeyRangeBound) string {
	// TODO implement
	panic("implement me")
}

func ResolvePreparedTransaction(ctx context.Context, sh, tx string, commit bool) {
	if shards == nil {
		err := LoadConfig(config.CoordinatorConfig().ShardDataCfg)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error loading config")
		}
	}

	db, err := pgx.Connect(ctx, createConnString(sh))
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error connecting to shard")
	}

	if commit {
		_, err = db.Exec(ctx, fmt.Sprintf("COMMIT PREPARED '%s'", tx))
	} else {
		_, err = db.Exec(ctx, fmt.Sprintf("ROLLBACK PREPARED '%s'", tx))
	}

	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error closing transaction")
	}
}

func beginTransactions(ctx context.Context, from, to pgxConnIface) (pgx.Tx, pgx.Tx, error) {
	txFrom, err := from.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error begining transaction")
		return nil, nil, err
	}
	txTo, err := to.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error begining transaction")
		return nil, nil, err
	}
	return txFrom, txTo, nil
}

func commitTransactions(ctx context.Context, f, t string, krid string, txTo, txFrom pgx.Tx, db qdb.XQDB) error {
	_, err := txTo.Exec(ctx, fmt.Sprintf("PREPARE TRANSACTION '%s-%s'", t, krid))
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error preparing transaction")
		return err
	}
	_, err = txFrom.Exec(ctx, fmt.Sprintf("PREPARE TRANSACTION '%s-%s'", f, krid))
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error preparing transaction")
		_, err1 := txTo.Exec(ctx, fmt.Sprintf("ROLLBACK PREPARED '%s-%s'", t, krid))
		if err1 != nil {
			spqrlog.Zero.Error().Err(err1).Msg("error closing transaction")
		}
		return err
	}

	d := qdb.DataTransferTransaction{
		ToShardId:   t,
		ToTxName:    fmt.Sprintf("%s-%s", t, krid),
		FromTxName:  fmt.Sprintf("%s-%s", f, krid),
		FromShardId: f,
		ToStatus:    qdb.Processing,
		FromStatus:  qdb.Processing,
	}

	err = db.RecordTransferTx(ctx, krid, &d)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error writing to qdb")
	}

	_, err = txTo.Exec(ctx, fmt.Sprintf("COMMIT PREPARED '%s-%s'", t, krid))
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error closing transaction")
		return err
	}

	d.ToStatus = qdb.Committed
	err = db.RecordTransferTx(ctx, krid, &d)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error writing to qdb")
	}

	_, err = txFrom.Exec(ctx, fmt.Sprintf("COMMIT PREPARED '%s-%s'", f, krid))
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error closing transaction")
		return err
	}

	d.FromStatus = qdb.Committed
	err = db.RecordTransferTx(ctx, krid, &d)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error removing from qdb")
	}
	return nil
}

func rollbackTransactions(ctx context.Context, txTo, txFrom pgx.Tx) error {
	err := txTo.Rollback(ctx)
	if err != nil {
		spqrlog.Zero.Warn().Msg("error closing transaction")
	}
	err1 := txFrom.Rollback(ctx)
	if err1 != nil {
		spqrlog.Zero.Warn().Msg("error closing transaction")
		return err1
	}
	return err
}

// TODO enhance for multi-column sharding rules
func moveData(ctx context.Context, keyRange, nextKeyRange *kr.KeyRange, rels map[string]*distributions.DistributedRelation, txTo, txFrom pgx.Tx) error {
	// TODO: use whole RFQN
	rows, err := txFrom.Query(ctx, `
SELECT table_name
FROM information_schema.tables;
`)
	if err != nil {
		return err
	}
	res := make(map[string]struct{})
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return err
		}

		res[tableName] = struct{}{}
	}

	rows.Close()

	for _, rel := range rels {
		if _, ok := res[strings.ToLower(rel.Name)]; !ok {
			continue
		}
		r, w, err := os.Pipe()
		if err != nil {
			return err
		}

		pw := ProxyW{
			w: w,
		}

		// This code does not work for multi-column key ranges.
		var qry string
		if nextKeyRange == nil {
			qry = fmt.Sprintf("COPY (DELETE FROM %s WHERE %s >= %s RETURNING *) TO STDOUT", rel.Name,
				rel.DistributionKey[0].Column, keyRange.LowerBound)
		} else {
			qry = fmt.Sprintf("COPY (DELETE FROM %s WHERE %s >= %s and %s < %s RETURNING *) TO STDOUT", rel.Name,
				rel.DistributionKey[0].Column, keyRange.LowerBound, rel.DistributionKey[0].Column, nextKeyRange.LowerBound)
		}

		go func() {
			_, err = txFrom.Conn().PgConn().CopyTo(ctx, &pw, qry)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
			}

			if err := pw.w.Close(); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("error closing pipe")
			}
		}()
		_, err = txTo.Conn().PgConn().CopyFrom(ctx,
			r, fmt.Sprintf("COPY %s FROM STDIN", rel.Name))
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("copy in failed")
			return err
		}
	}

	return nil
}
