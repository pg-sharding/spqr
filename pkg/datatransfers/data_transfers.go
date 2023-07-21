package datatransfers

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
)

type MoveTableRes struct {
	TableSchema string `db:"table_schema"`
	TableName   string `db:"table_name"`
}

// TODO: use schema
// var schema = flag.String("shema", "", "")

type ProxyW struct {
	w io.WriteCloser
}

var shards *config.DatatransferConnections
var txFrom pgx.Tx
var txTo pgx.Tx
var remoteConfigDir = "/spqr/docker/coordinator/shard_data.yaml"
var localConfigDir = "/pkg/datatransfers/shard_data.yaml"

func createConnString(shardID string) string {
	sd := shards.ShardsData[shardID]
	return fmt.Sprintf("user=%s host=%s port=%s dbname=%s password=%s", sd.User, sd.Host, sd.Port, sd.DB, sd.Password)
}

func LoadConfig() error {
	var err error
	shards, err = config.LoadShardDataCfg(remoteConfigDir)
	if err != nil {
		p, _ := os.Getwd()
		shards, err = config.LoadShardDataCfg(p + localConfigDir)
		if err != nil {
			return err
		}
	}
	return nil
}

func BeginTransactions(ctx context.Context, f, t string) error {
	if shards == nil {
		err := LoadConfig()
		if err != nil {
			return err
		}
	}

	from, err := pgx.Connect(ctx, createConnString(f))
	if err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "error connecting to shard: %v", err)
		return err
	}
	to, err := pgx.Connect(ctx, createConnString(t))
	if err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "error connecting to shard: %v", err)
		return err
	}

	txFrom, err = from.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "error begining transaction: %v", err)
		return err
	}
	txTo, err = to.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "error begining transaction: %v", err)
		return err
	}
	return nil
}

func CommitTransactions(ctx context.Context) error {
	err := txTo.Commit(ctx)
	if err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "error closing transaction: %v", err)
		return err
	}
	err = txFrom.Commit(ctx)
	if err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "error closing transaction: %v", err)
		return err
	}
	return nil
}

func RollbackTransactions(ctx context.Context) error {
	err := txTo.Rollback(ctx)
	if err != nil {
		spqrlog.Logger.Printf(spqrlog.WARNING, "error closing transaction: %v", err)
		return err
	}
	err = txFrom.Rollback(ctx)
	if err != nil {
		spqrlog.Logger.Printf(spqrlog.WARNING, "error closing transaction: %v", err)
		return err
	}
	return nil
}

func MoveKeys(ctx context.Context, keyr qdb.KeyRange, shr *shrule.ShardingRule) error {
	return moveData(ctx, *kr.KeyRangeFromDB(&keyr), shr)
}

func (p *ProxyW) Write(bt []byte) (int, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "got bytes %v", bt)
	return p.w.Write(bt)
}

func moveData(ctx context.Context, keyRange kr.KeyRange, key *shrule.ShardingRule) error {
	rows, err := txFrom.Query(ctx, `
SELECT table_schema, table_name
FROM information_schema.columns
WHERE column_name=$1;
`, key.Entries()[0].Column)
	if err != nil {
		return err
	}
	var ress []MoveTableRes
	for rows.Next() {
		var curres MoveTableRes
		err = rows.Scan(&curres.TableSchema, &curres.TableName)
		if err != nil {
			return err
		}

		ress = append(ress, curres)
	}

	rows.Close()

	for _, v := range ress {
		spqrlog.Logger.Printf(spqrlog.DEBUG3, "moving table %s:%s", v.TableSchema, v.TableName)

		r, w, err := os.Pipe()
		if err != nil {
			return err
		}

		pw := ProxyW{
			w: w,
		}
		qry := fmt.Sprintf("copy (delete from %s.%s WHERE %s >= %s and %s <= %s returning *) to stdout", v.TableSchema, v.TableName,
			key.Entries()[0].Column, keyRange.LowerBound, key.Entries()[0].Column, keyRange.UpperBound)

		spqrlog.Logger.Printf(spqrlog.DEBUG3, "executing %v", qry)

		_, err = txFrom.Conn().PgConn().CopyTo(ctx, &pw, qry)
		if err != nil {
			spqrlog.Logger.PrintError(err)
		}

		if err := pw.w.Close(); err != nil {
			spqrlog.Logger.Printf(spqrlog.ERROR, "error closing pipe %v", err)
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG3, "sending rows to dest shard")
		_, err = txTo.Conn().PgConn().CopyFrom(ctx,
			r, fmt.Sprintf("COPY %s.%s FROM STDIN", v.TableSchema, v.TableName))
		if err != nil {
			spqrlog.Logger.Printf(spqrlog.ERROR, "copy in failed %v", err)
			return err
		}
	}

	return nil
}
