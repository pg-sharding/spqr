package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v4"
	_ "github.com/lib/pq"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"io"
	"os"
)

type MoveTableRes struct {
	TableSchema string `db:"table_schema"`
	TableName   string `db:"table_name"`
}

var fromShardConnst = flag.String("from-shard-connstring", "", "")
var toShardConnst = flag.String("to-shard-connstring", "", "")
var lb = flag.String("lower-bound", "", "")
var ub = flag.String("upper-bound", "", "")
var shkey = flag.String("sharding-key", "", "")

// TODO: use schema
var schema = flag.String("shema", "", "")

type ProxyW struct {
	w io.WriteCloser
}

func (p *ProxyW) Write(bt []byte) (int, error) {
	spqrlog.Logger.Printf(spqrlog.ERROR, "got bytes %v", bt)
	return p.w.Write(bt)
}

func moveData(ctx context.Context, from, to *pgx.Conn, keyRange kr.KeyRange, key *shrule.ShardingRule) error {
	txFrom, err := from.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	txTo, err := to.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func(txTo pgx.Tx) {
		err := txTo.Rollback(ctx)
		if err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}(txTo)

	defer func(txFrom pgx.Tx) {
		err := txFrom.Rollback(ctx)
		if err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}(txFrom)

	rows, err := txFrom.Query(ctx, `
SELECT table_schema, table_name
FROM information_schema.columns
WHERE column_name=$1;
`, key.Columns()[0])
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
		spqrlog.Logger.Printf(spqrlog.ERROR, "moving table %s:%s", v.TableSchema, v.TableName)

		r, w, err := os.Pipe()
		if err != nil {
			return err
		}

		pw := ProxyW{
			w: w,
		}

		ch := make(chan struct{})

		go func() {
			spqrlog.Logger.Printf(spqrlog.ERROR, "sending rows to dest shard")
			_, err := txTo.Conn().PgConn().CopyFrom(ctx,
				r, fmt.Sprintf("COPY %s.%s FROM STDIN", v.TableSchema, v.TableName))
			if err != nil {
				spqrlog.Logger.Printf(spqrlog.ERROR, "copy in failed %v", err)
			}

			ch <- struct{}{}
		}()

		qry := fmt.Sprintf("copy (delete from %s.%s WHERE %s >= %s and %s <= %s returning *) to stdout", v.TableSchema, v.TableName,
			key.Columns()[0], keyRange.LowerBound, key.Columns()[0], keyRange.UpperBound)

		spqrlog.Logger.Printf(spqrlog.ERROR, "executing %v", qry)

		_, err = txFrom.Conn().PgConn().CopyTo(ctx, &pw, qry)
		if err != nil {
			spqrlog.Logger.PrintError(err)
		}

		if err := pw.w.Close(); err != nil {
			spqrlog.Logger.Printf(spqrlog.ERROR, "error closing pipe %v", err)
		}

		spqrlog.Logger.Printf(spqrlog.ERROR, "copy cmd executed")

		<-ch
	}

	_ = txTo.Commit(ctx)
	_ = txFrom.Commit(ctx)
	return nil
}

func main() {
	flag.Parse()

	ctx := context.Background()

	connFrom, err := pgx.Connect(ctx, *fromShardConnst)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return
	}

	connTo, err := pgx.Connect(ctx, *toShardConnst)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return
	}

	if err := moveData(ctx,
		connFrom, connTo, kr.KeyRange{LowerBound: []byte(*lb), UpperBound: []byte(*ub)},
		shrule.NewShardingRule([]string{*shkey})); err != nil {
		spqrlog.Logger.PrintError(err)
	}
}
