package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
)

type MoveTableRes struct {
	TableSchema string `db:"table_schema"`
	TableName   string `db:"table_name"`
}

var fromShardConnst = flag.String("from-shard-connstring", "", "")
var toShardConnst = flag.String("to-shard-connstring", "", "")

// TODO: deprecate this API, use IDS
var lb = flag.String("lower-bound", "", "")
var shkey = flag.String("sharding-key", "", "")
var etcdAddr = flag.String("etcd-addr", "", "")

// TODO: use schema
// var schema = flag.String("shema", "", "")

type ProxyW struct {
	w io.WriteCloser
}

func (p *ProxyW) Write(bt []byte) (int, error) {
	spqrlog.Zero.Debug().
		Bytes("bytes", bt).
		Msg("got bytes")
	return p.w.Write(bt)
}

// TODO : unit tests
func moveData(ctx context.Context, from, to *pgx.Conn, keyRange, nextKeyRange *kr.KeyRange, key *shrule.ShardingRule) error {
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
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}(txTo)

	defer func(txFrom pgx.Tx) {
		err := txFrom.Rollback(ctx)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}(txFrom)

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
		spqrlog.Zero.Debug().
			Str("schema", v.TableSchema).
			Str("table", v.TableName).
			Msg("moving table")

		r, w, err := os.Pipe()
		if err != nil {
			return err
		}

		pw := ProxyW{
			w: w,
		}

		var qry string

		// TODO: support multi-column move in SPQR2
		if nextKeyRange == nil {

			qry = fmt.Sprintf("copy (delete from %s.%s WHERE %s >= %s returning *) to stdout", v.TableSchema, v.TableName,
				key.Entries()[0].Column, keyRange.LowerBound)
		} else {
			qry = fmt.Sprintf("copy (delete from %s.%s WHERE %s >= %s and %s < %s returning *) to stdout", v.TableSchema, v.TableName,
				key.Entries()[0].Column, keyRange.LowerBound, key.Entries()[0].Column, nextKeyRange.LowerBound)
		}

		spqrlog.Zero.Debug().
			Str("query", qry).
			Msg("executing query")

		_, err = txFrom.Conn().PgConn().CopyTo(ctx, &pw, qry)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}

		if err := pw.w.Close(); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error closing pipe")
		}

		_, err = txTo.Conn().PgConn().CopyFrom(ctx,
			r, fmt.Sprintf("COPY %s.%s FROM STDIN", v.TableSchema, v.TableName))
		if err != nil {
			spqrlog.Zero.Debug().Msg("copy in failed")
			return err
		}

		spqrlog.Zero.Debug().Msg("copy cmd executed")
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
		spqrlog.Zero.Error().Err(err).Msg("")
		return
	}

	connTo, err := pgx.Connect(ctx, *toShardConnst)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return
	}

	db, err := qdb.NewEtcdQDB(*etcdAddr)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return
	}

	shRule, err := db.GetShardingRule(context.TODO(), *shkey)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return
	}

	var keyRange, nextKeyRange *kr.KeyRange

	krs, err := db.ListAllKeyRanges(ctx)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return
	}

	for _, currkr := range krs {
		if kr.CmpRangesEqual(currkr.LowerBound, []byte(*lb)) {
			keyRange = kr.KeyRangeFromDB(currkr)
		}
	}

	for _, currkr := range krs {
		if kr.CmpRangesLess(keyRange.LowerBound, currkr.LowerBound) {
			if nextKeyRange == nil || kr.CmpRangesLess(currkr.LowerBound, nextKeyRange.LowerBound) {
				nextKeyRange = kr.KeyRangeFromDB(currkr)
			}
		}
	}

	if keyRange == nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to resolve key range by its bound")
		return
	}

	if err := moveData(ctx,
		connFrom, connTo, keyRange, nextKeyRange,
		shrule.ShardingRuleFromDB(shRule)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
	}
}
