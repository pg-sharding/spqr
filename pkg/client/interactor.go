package client

import (
	"context"
	"fmt"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"net"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
)

type Interactor interface {
	ProcClient(ctx context.Context, conn net.Conn) error
}

type PSQLInteractor struct{}

func (pi *PSQLInteractor) completeMsg(rowCnt int, cl Client) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", rowCnt))},
		&pgproto3.ReadyForQuery{
			TxStatus: byte(conn.TXIDLE),
		},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return nil
}

func (pi *PSQLInteractor) WriteHeader(stmt string, cl Client) error {
	return cl.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
		{
			Name:                 []byte(stmt),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          25,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		},
	},
	})
}

func (pi *PSQLInteractor) Databases(dbs []string, cl Client) error {
	if err := pi.WriteHeader("show dbs", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte("show dbs")}},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	for _, db := range dbs {
		if err := cl.Send(&pgproto3.DataRow{
			Values: [][]byte{[]byte(fmt.Sprintf("database %s", db))},
		}); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(len(dbs), cl)
}

func (pi *PSQLInteractor) Pools(cl Client) error {
	if err := pi.WriteHeader("show pools", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte("show pools")}},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) AddShard(cl Client, shard *datashards.DataShard) error {
	if err := pi.WriteHeader("add datashard", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("created datashard with name %s", shard.ID))}},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) KeyRanges(krs []*kr.KeyRange, cl Client) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "listing key ranges")
	if err := pi.WriteHeader("listing key ranges", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, keyRange := range krs {
		if err := cl.Send(&pgproto3.DataRow{
			Values: [][]byte{[]byte(fmt.Sprintf("key range %v mapped to datashard %s", keyRange.ID, keyRange.ShardID))},
		}); err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}

	return pi.completeMsg(len(krs), cl)
}

func (pi *PSQLInteractor) AddKeyRange(ctx context.Context, keyRange *kr.KeyRange, cl Client) error {
	if err := pi.WriteHeader("add key range", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("created key range from %s to %s", keyRange.LowerBound, keyRange.UpperBound))}},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) SplitKeyRange(ctx context.Context, split *kr.SplitKeyRange, cl Client) error {
	if err := pi.WriteHeader("split key range", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("split key range %v by %v", split.SourceID, split.Bound))}},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) LockKeyRange(ctx context.Context, krid string, cl Client) error {
	if err := pi.WriteHeader("lock key range", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{
			[]byte(fmt.Sprintf("lock key range with id %v", krid))},
		},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) UnlockKeyRange(ctx context.Context, krid string, cl Client) error {
	if err := pi.WriteHeader("unlock key range", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{
			[]byte(
				fmt.Sprintf("unlocked key range with id %v", krid)),
		},
		},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) Shards(ctx context.Context, shards []*datashards.DataShard, cl Client) error {
	if err := pi.WriteHeader("listing data shards", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "listing shards")

	for _, shard := range shards {
		if err := cl.Send(&pgproto3.DataRow{
			Values: [][]byte{[]byte(fmt.Sprintf("datashard with ID %s", shard))},
		}); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) ShardingRules(ctx context.Context, rules []*shrule.ShardingRule, cl Client) error {
	if err := pi.WriteHeader("listing sharding rules", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "listing sharding rules")

	for _, rule := range rules {
		if err := cl.Send(&pgproto3.DataRow{
			Values: [][]byte{[]byte(fmt.Sprintf("colmns-match sharding rule with colmn set: %+v", rule.Columns()))},
		}); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) ReportError(err error, cl Client) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ErrorResponse{Severity: "ERROR",
			Message: err.Error(),
		},
		&pgproto3.ReadyForQuery{
			TxStatus: byte(conn.TXIDLE),
		},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return nil
}

func (pi *PSQLInteractor) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule, cl Client) error {
	if err := pi.WriteHeader("add sharding rule", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("created sharding column %s", rule.Columns()))}},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) MoveKeyRange(ctx context.Context, move *kr.MoveKeyRange, cl Client) error {
	if err := pi.WriteHeader("move key range", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("moved key range %s to %s", move.Krid, move.ShardId))}},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}
