package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pg-sharding/spqr/pkg/models/distributions"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
)

var fromShardConnSt = flag.String("from-shard-connstring", "", "Connection string to shard to move data from")
var toShardConnSt = flag.String("to-shard-connstring", "", "Connection string to shard to move data to")

var krId = flag.String("key-range", "", "ID of key range to move")
var etcdAddr = flag.String("etcd-addr", "", "ETCD address")

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
func moveData(ctx context.Context, from, to *pgx.Conn, keyRange, nextKeyRange *kr.KeyRange, rels map[string]*distributions.DistributedRelation) error {
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
		spqrlog.Zero.Debug().
			Str("relation", rel.Name).
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
			qry = fmt.Sprintf("copy (delete from %s WHERE %s >= %s returning *) to stdout", rel.Name,
				rel.DistributionKey[0].Column, keyRange.SendRaw()[0])
		} else {
			qry = fmt.Sprintf("copy (delete from %s WHERE %s >= %s and %s < %s returning *) to stdout", rel.Name,
				rel.DistributionKey[0].Column, keyRange.SendRaw()[0], rel.DistributionKey[0].Column, nextKeyRange.SendRaw()[0])
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
			r, fmt.Sprintf("COPY %s FROM STDIN", rel.Name))
		if err != nil {
			spqrlog.Zero.Debug().Msg("copy in failed")
			return err
		}

		spqrlog.Zero.Debug().Msg("copy cmd executed")
	}

	/* TODO: handle errors here */
	_ = txTo.Commit(ctx)
	_ = txFrom.Commit(ctx)
	return nil
}

func main() {
	flag.Parse()

	ctx := context.Background()

	connFrom, err := pgx.Connect(ctx, *fromShardConnSt)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return
	}

	connTo, err := pgx.Connect(ctx, *toShardConnSt)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return
	}

	db, err := qdb.NewEtcdQDB(*etcdAddr, 0)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return
	}

	qdbKr, err := db.GetKeyRange(ctx, *krId)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return
	}

	ds, err := db.GetDistribution(ctx, qdbKr.DistributionId)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return
	}

	keyRange := kr.KeyRangeFromDB(qdbKr, ds.ColTypes)

	krs, err := db.ListKeyRanges(ctx, keyRange.Distribution)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return
	}

	var nextKeyRange *kr.KeyRange

	for _, currkr := range krs {
		typedKr := kr.KeyRangeFromDB(currkr, ds.ColTypes)
		if kr.CmpRangesLess(keyRange.LowerBound, typedKr.LowerBound, ds.ColTypes) {
			if nextKeyRange == nil || kr.CmpRangesLess(typedKr.LowerBound, nextKeyRange.LowerBound, ds.ColTypes) {
				nextKeyRange = typedKr
			}
		}
	}

	dbDs, err := db.GetDistribution(ctx, keyRange.Distribution)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return
	}

	if err := moveData(ctx,
		connFrom, connTo, keyRange, nextKeyRange,
		distributions.DistributionFromDB(dbDs).Relations); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
	}
}
