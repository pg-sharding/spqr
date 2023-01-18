package clientinteractor

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/pg-sharding/spqr/pkg/models/dataspaces"
	"github.com/pg-sharding/spqr/pkg/models/topology"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgproto3/v2"

	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
)

type Interactor interface {
	ProcClient(ctx context.Context, nconn net.Conn) error
}

type PSQLInteractor struct {
	cl client.Client
}

func NewPSQLInteractor(cl client.Client) *PSQLInteractor {
	return &PSQLInteractor{
		cl: cl,
	}
}

func (pi *PSQLInteractor) CompleteMsg(rowCnt int) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", rowCnt))},
		&pgproto3.ReadyForQuery{
			TxStatus: byte(conn.TXIDLE),
		},
	} {
		if err := pi.cl.Send(msg); err != nil {
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

func (pi *PSQLInteractor) WriteHeader(stmt string) error {
	return pi.cl.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{TextOidFD(stmt)}})
}

func (pi *PSQLInteractor) WriteDataRow(msg string) error {
	return pi.cl.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(msg)}})
}

func (pi *PSQLInteractor) Databases(dbs []string) error {
	if err := pi.WriteHeader("show databases"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []string{
		"show dbs",
	} {
		if err := pi.WriteDataRow(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	for _, db := range dbs {
		if err := pi.cl.Send(&pgproto3.DataRow{
			Values: [][]byte{[]byte(fmt.Sprintf("database %s", db))},
		}); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.CompleteMsg(len(dbs))
}

func (pi *PSQLInteractor) Pools() error {
	if err := pi.WriteHeader("show pools"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []string{
		"show pools",
	} {
		if err := pi.WriteDataRow(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) AddShard(shard *datashards.DataShard) error {
	if err := pi.WriteHeader("add datashard"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("created datashard with name %s", shard.ID))}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) KeyRanges(krs []*kr.KeyRange) error {
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
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	for _, keyRange := range krs {
		if err := pi.cl.Send(&pgproto3.DataRow{
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
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return nil
}

func (pi *PSQLInteractor) AddKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {
	if err := pi.WriteHeader("add key range"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("created key range from %s to %s", keyRange.LowerBound, keyRange.UpperBound))}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) SplitKeyRange(ctx context.Context, split *kr.SplitKeyRange) error {
	if err := pi.WriteHeader("split key range"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("split key range %v by %s", split.SourceID, string(split.Bound)))}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) LockKeyRange(ctx context.Context, krid string) error {
	if err := pi.WriteHeader("lock key range"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{
			[]byte(fmt.Sprintf("lock key range with id %v", krid))},
		},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) UnlockKeyRange(ctx context.Context, krid string) error {
	if err := pi.WriteHeader("unlock key range"); err != nil {
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
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) Shards(ctx context.Context, shards []*datashards.DataShard) error {
	if err := pi.WriteHeader("listing data shards"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "listing shards")

	for _, shard := range shards {
		if err := pi.cl.Send(&pgproto3.DataRow{
			Values: [][]byte{[]byte(fmt.Sprintf("datashard with ID %s", shard.ID))},
		}); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) ShardingRules(ctx context.Context, rules []*shrule.ShardingRule) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "listing sharding rules")

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			TextOidFD("Sharding Rule ID"),
			TextOidFD("Table Name"),
			TextOidFD("Columns"),
			TextOidFD("Hash Function"),
		}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	for _, rule := range rules {
		var entries strings.Builder
		var hashFunctions strings.Builder
		for _, entry := range rule.Entries() {
			entries.WriteString(entry.Column)

			if entry.HashFunction == "" {
				hashFunctions.WriteString("x->x")
			} else {
				hashFunctions.WriteString(entry.HashFunction)
			}
		}
		tableName := "*"
		if rule.TableName != "" {
			tableName = rule.TableName
		}

		if err := pi.cl.Send(&pgproto3.DataRow{
			Values: [][]byte{
				[]byte(rule.Id),
				[]byte(tableName),
				[]byte(entries.String()),
				[]byte(hashFunctions.String()),
			},
		}); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) ReportError(err error) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ErrorResponse{Severity: "ERROR",
			Message: err.Error(),
		},
		&pgproto3.ReadyForQuery{
			TxStatus: byte(conn.TXIDLE),
		},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return nil
}

func (pi *PSQLInteractor) DropShardingRule(ctx context.Context, id string) error {
	if err := pi.WriteHeader("drop sharding rule"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("dropped sharding rule %s", id)); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}
	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	if err := pi.WriteHeader("add sharding rule"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("created %s", rule.String())); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}
	return pi.CompleteMsg(0)
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

func (pi *PSQLInteractor) MoveKeyRange(_ context.Context, move *kr.MoveKeyRange) error {
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
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}

	return nil
}

func (pi *PSQLInteractor) Routers(resp []*topology.Router) error {
	if err := pi.WriteHeader("show routers"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, msg := range resp {
		if err := pi.WriteDataRow(fmt.Sprintf("router %s-%s", msg.Id, msg.AdmAddr)); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) UnregisterRouter(id string) error {
	if err := pi.WriteHeader("unregister routers"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("router %s unregistered", id)); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) RegisterRouter(ctx context.Context, id string, addr string) error {
	if err := pi.WriteHeader("register routers"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("router %s-%s registered", id, addr)); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) DropKeyRange(ctx context.Context, ids []string) error {
	if err := pi.WriteHeader("drop key range"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for _, id := range ids {
		if err := pi.WriteDataRow(fmt.Sprintf("drop key range %s", id)); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) AddDataspace(ctx context.Context, ks *dataspaces.Dataspace) error {
	if err := pi.WriteHeader("add dataspace"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("created dataspace with id %s", ks.ID())); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}
	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) ReportStmtRoutedToAllShards(ctx context.Context) error {
	if err := pi.WriteHeader("explain query"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("query routed to all shards (multishard)")); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}
	return pi.CompleteMsg(0)
}
