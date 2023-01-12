package clientinteractor

import (
	"context"
	"fmt"
	"github.com/pg-sharding/spqr/pkg/models/dataspaces"
	"net"

	"github.com/pg-sharding/spqr/pkg/models/routers"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgproto3/v2"

	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
)

type Interactor interface {
	ProcClient(ctx context.Context, conn net.Conn) error
}

type PSQLInteractor struct{}

func (pi *PSQLInteractor) completeMsg(rowCnt int, cl client.Client) error {
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

// TEXTOID https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat#L81
const TEXTOID = 25

func TextOidFD(stmt string) pgproto3.FieldDescription {
	return pgproto3.FieldDescription{
		Name:                 []byte(stmt),
		TableOID:             0,
		TableAttributeNumber: 0,
		DataTypeOID:          TEXTOID,
		DataTypeSize:         -1,
		TypeModifier:         -1,
		Format:               0,
	}
}

func (pi *PSQLInteractor) WriteHeader(stmt string, cl client.Client) error {
	return cl.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{TextOidFD(stmt)}})
}

func (pi *PSQLInteractor) WriteDataRow(msg string, cl client.Client) error {
	return cl.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(msg)}})
}

func (pi *PSQLInteractor) Databases(dbs []string, cl client.Client) error {
	if err := pi.WriteHeader("show databases", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []string{
		"show dbs",
	} {
		if err := pi.WriteDataRow(msg, cl); err != nil {
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

func (pi *PSQLInteractor) Pools(cl client.Client) error {
	if err := pi.WriteHeader("show pools", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []string{
		"show pools",
	} {
		if err := pi.WriteDataRow(msg, cl); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) AddShard(cl client.Client, shard *datashards.DataShard) error {
	if err := pi.WriteHeader("add datashard", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("created datashard with name %s", shard.ID))}},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) KeyRanges(krs []*kr.KeyRange, cl client.Client) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "listing key ranges")

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			TextOidFD("Key range ID"),
			TextOidFD("Shard ID"),
			TextOidFD("Lower bound"),
			TextOidFD("Upper bound"),
		},
		},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	for _, keyRange := range krs {
		if err := cl.Send(&pgproto3.DataRow{
			Values: [][]byte{
				[]byte(keyRange.ID),
				[]byte(keyRange.ShardID),
				keyRange.LowerBound,
				keyRange.UpperBound,
			},
		}); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.CommandComplete{},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return nil
}

func (pi *PSQLInteractor) AddKeyRange(ctx context.Context, keyRange *kr.KeyRange, cl client.Client) error {
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

func (pi *PSQLInteractor) SplitKeyRange(ctx context.Context, split *kr.SplitKeyRange, cl client.Client) error {
	if err := pi.WriteHeader("split key range", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("split key range %v by %s", split.SourceID, string(split.Bound)))}},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) LockKeyRange(ctx context.Context, krid string, cl client.Client) error {
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

func (pi *PSQLInteractor) UnlockKeyRange(ctx context.Context, krid string, cl client.Client) error {
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

func (pi *PSQLInteractor) Shards(ctx context.Context, shards []*datashards.DataShard, cl client.Client) error {
	if err := pi.WriteHeader("listing data shards", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "listing shards")

	for _, shard := range shards {
		if err := cl.Send(&pgproto3.DataRow{
			Values: [][]byte{[]byte(fmt.Sprintf("datashard with ID %s", shard.ID))},
		}); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) ShardingRules(ctx context.Context, rules []*shrule.ShardingRule, cl client.Client) error {
	if err := pi.WriteHeader("listing sharding rules", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "listing sharding rules")

	for _, rule := range rules {

		if err := pi.WriteDataRow(fmt.Sprintf("sharding rule %v with column set: %+v", rule.Id, rule.Entries()), cl); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) ReportError(err error, cl client.Client) error {
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

func (pi *PSQLInteractor) DropShardingRule(ctx context.Context, id string, cl client.Client) error {
	if err := pi.WriteHeader("drop sharding rule", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("dropped sharding rule %s", id), cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}
	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule, cl client.Client) error {
	if err := pi.WriteHeader("add sharding rule", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("created sharding column %s", rule.Entries()), cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}
	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) MergeKeyRanges(_ context.Context, unite *kr.UniteKeyRange, cl client.Client) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 []byte("merge key ranges"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		},
		},
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("merge key ranges %v and %v", unite.KeyRangeIDLeft, unite.KeyRangeIDRight))}},
		&pgproto3.CommandComplete{},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}

	return nil
}

func (pi *PSQLInteractor) MoveKeyRange(_ context.Context, move *kr.MoveKeyRange, cl client.Client) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 []byte("move key range"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		},
		},
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("move key range %v to shard %v", move.Krid, move.ShardId))}},
		&pgproto3.CommandComplete{},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}

	return nil
}

func (pi *PSQLInteractor) Routers(resp []*routers.Router, cl client.Client) error {
	if err := pi.WriteHeader("show routers", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range resp {
		if err := pi.WriteDataRow(fmt.Sprintf("router %s-%s", msg.Id, msg.AdmAddr), cl); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) UnregisterRouter(cl client.Client, id string) error {
	if err := pi.WriteHeader("unregister routers", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("router %s unregistered", id), cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) RegisterRouter(ctx context.Context, cl client.Client, id string, addr string) error {
	if err := pi.WriteHeader("register routers", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("router %s-%s registered", id, addr), cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) DropKeyRange(ctx context.Context, ids []string, cl client.Client) error {
	if err := pi.WriteHeader("drop key range", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, id := range ids {
		if err := pi.WriteDataRow(fmt.Sprintf("drop key range %s", id), cl); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.completeMsg(0, cl)
}

func (pi *PSQLInteractor) AddDataspace(ctx context.Context, ks *dataspaces.Dataspace, cl client.Client) error {
	if err := pi.WriteHeader("add dataspace", cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("created dataspace with id %s", ks.ID()), cl); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}
	return pi.completeMsg(0, cl)
}
