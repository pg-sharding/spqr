package clientinteractor

import (
	"context"
	"fmt"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/statistics"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
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

// TODO : unit tests
func (pi *PSQLInteractor) CompleteMsg(rowCnt int) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", rowCnt))},
		&pgproto3.ReadyForQuery{
			TxStatus: byte(txstatus.TXIDLE),
		},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return nil
}

// TEXTOID https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat#L81
const TEXTOID = 25

// TODO : unit tests
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

// TODO : unit tests
func (pi *PSQLInteractor) WriteHeader(stmts ...string) error {
	var desc []pgproto3.FieldDescription
	for _, stmt := range stmts {
		desc = append(desc, TextOidFD(stmt))
	}
	return pi.cl.Send(&pgproto3.RowDescription{Fields: desc})
}

// TODO : unit tests
func (pi *PSQLInteractor) WriteDataRow(msgs ...string) error {
	vals := make([][]byte, 0)
	for _, msg := range msgs {
		vals = append(vals, []byte(msg))
	}
	return pi.cl.Send(&pgproto3.DataRow{Values: vals})
}

// TODO : unit tests
func (pi *PSQLInteractor) Databases(dbs []string) error {
	if err := pi.WriteHeader("show databases"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []string{
		"show dbs",
	} {
		if err := pi.WriteDataRow(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	for _, db := range dbs {
		if err := pi.cl.Send(&pgproto3.DataRow{
			Values: [][]byte{[]byte(fmt.Sprintf("database %s", db))},
		}); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(len(dbs))
}

// TODO : unit tests
func (pi *PSQLInteractor) Pools(_ context.Context, ps []pool.Pool) error {
	if err := pi.WriteHeader(
		"pool id",
		"pool router",
		"pool db",
		"pool usr",
		"pool host",
		"used connection count",
		"idle connections",
		"queue residual size"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	for _, p := range ps {
		if err := pi.WriteDataRow(
			fmt.Sprintf("%p", p),
			p.RouterName(),
			p.Rule().DB,
			p.Rule().Usr,
			p.Hostname(),
			fmt.Sprintf("%d", p.UsedConnectionCount()),
			fmt.Sprintf("%d", p.IdleConnectionCount()),
			fmt.Sprintf("%d", p.QueueResidualSize())); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(len(ps))
}

// TODO : unit tests
func (pi *PSQLInteractor) Version(_ context.Context) error {
	if err := pi.WriteHeader("SPQR version"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	msg := &pgproto3.DataRow{Values: [][]byte{[]byte(pkg.SpqrVersionRevision)}}
	if err := pi.cl.Send(msg); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) AddShard(shard *datashards.DataShard) error {
	if err := pi.WriteHeader("add datashard"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("created datashard with name %s", shard.ID))}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) DropShard(id string) error {
	if err := pi.WriteHeader("drop shard"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("dropped shard with %s", id))}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) KeyRanges(krs []*kr.KeyRange) error {
	spqrlog.Zero.Debug().Msg("listing key ranges")

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			TextOidFD("Key range ID"),
			TextOidFD("Shard ID"),
			TextOidFD("Distribution ID"),
			TextOidFD("Lower bound"),
		},
		},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	for _, keyRange := range krs {
		if err := pi.cl.Send(&pgproto3.DataRow{
			Values: [][]byte{
				[]byte(keyRange.ID),
				[]byte(keyRange.ShardID),
				[]byte(keyRange.Distribution),
				keyRange.LowerBound,
			},
		}); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}
	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) AddKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {
	if err := pi.WriteHeader("add key range"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("created key range with bound %s", keyRange.LowerBound))}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) SplitKeyRange(ctx context.Context, split *kr.SplitKeyRange) error {
	if err := pi.WriteHeader("split key range"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("split key range %v by %s", split.SourceID, string(split.Bound)))}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) LockKeyRange(ctx context.Context, krid string) error {
	if err := pi.WriteHeader("lock key range"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{
			[]byte(fmt.Sprintf("lock key range with id %v", krid))},
		},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) UnlockKeyRange(ctx context.Context, krid string) error {
	if err := pi.WriteHeader("unlock key range"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
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
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) Shards(ctx context.Context, shards []*datashards.DataShard) error {
	if err := pi.WriteHeader("listing data shards"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	spqrlog.Zero.Debug().Msg("listing shards")

	for _, shard := range shards {
		if err := pi.cl.Send(&pgproto3.DataRow{
			Values: [][]byte{[]byte(fmt.Sprintf("datashard with ID %s", shard.ID))},
		}); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func MatchRow(row []string, nameToIndex map[string]int, condition spqrparser.WhereClauseNode) (bool, error) {
	if condition == nil {
		return true, nil
	}
	switch where := condition.(type) {
	case spqrparser.WhereClauseEmpty:
		return true, nil
	case spqrparser.WhereClauseOp:
		switch strings.ToLower(where.Op) {
		case "and":
			left, err := MatchRow(row, nameToIndex, where.Left)
			if err != nil {
				return true, err
			}
			if !left {
				return false, nil
			}
			right, err := MatchRow(row, nameToIndex, where.Right)
			if err != nil {
				return true, err
			}
			return right, nil
		case "or":
			left, err := MatchRow(row, nameToIndex, where.Left)
			if err != nil {
				return true, err
			}
			if left {
				return true, nil
			}
			right, err := MatchRow(row, nameToIndex, where.Right)
			if err != nil {
				return true, err
			}
			return right, nil
		default:
			return true, spqrerror.Newf(spqrerror.SPQR_COMPLEX_QUERY, "not supported logic operation: %s", where.Op)
		}
	case spqrparser.WhereClauseLeaf:
		switch where.Op {
		case "=":
			i, ok := nameToIndex[where.ColRef.ColName]
			if !ok {
				return true, spqrerror.Newf(spqrerror.SPQR_COMPLEX_QUERY, "column %s does not exist", where.ColRef.ColName)
			}
			return row[i] == where.Value, nil
		default:
			return true, spqrerror.Newf(spqrerror.SPQR_COMPLEX_QUERY, "not supported operation %s", where.Op)
		}
	default:
		return false, nil
	}
}

type TableDesc interface {
	GetHeader() []string
}

type ClientDesc struct {
}

// TODO : unit tests
func (ClientDesc) GetRow(cl client.Client, hostname string, rAddr string) []string {
	quantiles := statistics.GetQuantiles()
	rowData := []string{fmt.Sprintf("%d", cl.ID()), cl.Usr(), cl.DB(), hostname, rAddr}

	for _, el := range *quantiles {
		rowData = append(rowData, fmt.Sprintf("%.2fms", statistics.GetTimeQuantile(statistics.Router, el, cl.ID())))
		rowData = append(rowData, fmt.Sprintf("%.2fms", statistics.GetTimeQuantile(statistics.Shard, el, cl.ID())))
	}
	return rowData
}

// TODO : unit tests
func (ClientDesc) GetHeader() []string {
	quantiles := statistics.GetQuantiles()
	headers := []string{
		"client_id", "user", "dbname", "server_id", "router_address",
	}
	for _, el := range *quantiles {
		headers = append(headers, fmt.Sprintf("router_time_%g", el))
		headers = append(headers, fmt.Sprintf("shard_time_%g", el))
	}
	return headers
}

// TODO : unit tests
func GetColumnsMap(desc TableDesc) map[string]int {
	header := desc.GetHeader()
	columns := make(map[string]int, len(header))
	i := 0
	for _, key := range header {
		columns[key] = i
		i++
	}
	return columns
}

// TODO : unit tests
func (pi *PSQLInteractor) Clients(ctx context.Context, clients []client.ClientInfo, condition spqrparser.WhereClauseNode) error {
	desc := ClientDesc{}
	header := desc.GetHeader()
	rowDesc := GetColumnsMap(desc)

	if err := pi.WriteHeader(header...); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, cl := range clients {
		if len(cl.Shards()) > 0 {
			for _, sh := range cl.Shards() {
				if sh == nil {
					continue
				}
				row := desc.GetRow(cl, sh.Instance().Hostname(), cl.RAddr())

				match, err := MatchRow(row, rowDesc, condition)
				if err != nil {
					return err
				}
				if !match {
					continue
				}

				if err := pi.WriteDataRow(row...); err != nil {
					spqrlog.Zero.Error().Err(err).Msg("")
					return err
				}
			}
		} else {
			row := desc.GetRow(cl, "no backend connection", cl.RAddr())

			match, err := MatchRow(row, rowDesc, condition)
			if err != nil {
				return err
			}
			if !match {
				continue
			}

			if err := pi.WriteDataRow(row...); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
				return err
			}
		}

	}

	return pi.CompleteMsg(len(clients))
}

// TODO : unit tests
func (pi *PSQLInteractor) Distributions(_ context.Context, distributions []*distributions.Distribution) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			TextOidFD("Distribution ID"),
			TextOidFD("Column types"),
		}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}
	for _, distribution := range distributions {
		if err := pi.cl.Send(&pgproto3.DataRow{
			Values: [][]byte{
				[]byte(distribution.Id),
				[]byte(strings.Join(distribution.ColTypes, ",")),
			},
		}); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}
	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) ReportError(err error) error {
	if err == nil {
		return nil
	}
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ErrorResponse{Severity: "ERROR",
			Message: err.Error(),
		},
		&pgproto3.ReadyForQuery{
			TxStatus: byte(txstatus.TXIDLE),
		},
	} {
		if err := pi.cl.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

// TODO : unit tests
func (pi *PSQLInteractor) MergeKeyRanges(_ context.Context, unite *kr.UniteKeyRange) error {
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
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("merge key ranges %v and %v", unite.BaseKeyRangeId, unite.AppendageKeyRangeId))}},
		&pgproto3.CommandComplete{},
		&pgproto3.ReadyForQuery{},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}

	return nil
}

// TODO : unit tests
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
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}

	return nil
}

// TODO : unit tests
func (pi *PSQLInteractor) Routers(resp []*topology.Router) error {
	if err := pi.WriteHeader("show routers", "status"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range resp {
		if err := pi.WriteDataRow(fmt.Sprintf("router %s-%s", msg.ID, msg.Address), string(msg.State)); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) UnregisterRouter(id string) error {
	if err := pi.WriteHeader("unregister routers"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("router %s unregistered", id)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) RegisterRouter(ctx context.Context, id string, addr string) error {
	if err := pi.WriteHeader("register routers"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("router %s-%s registered", id, addr)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) StartTraceMessages(ctx context.Context) error {
	if err := pi.WriteHeader("start trace messages"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow("START TRASCE MESSAGES"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) StopTraceMessages(ctx context.Context) error {
	if err := pi.WriteHeader("stop trace messages"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow("STOP TRASCE MESSAGES"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) DropKeyRange(ctx context.Context, ids []string) error {
	if err := pi.WriteHeader("drop key range"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, id := range ids {
		if err := pi.WriteDataRow(fmt.Sprintf("drop key range %s", id)); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) AddDistribution(ctx context.Context, ks *distributions.Distribution) error {
	if err := pi.WriteHeader("add distribution"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("created distribution with id %s", ks.ID())); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) DropDistribution(ctx context.Context, ids []string) error {
	if err := pi.WriteHeader("drop distribution"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, id := range ids {
		if err := pi.WriteDataRow(fmt.Sprintf("drop distribution %s", id)); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) AlterDistributionAttach(ctx context.Context, id string, ds []*distributions.DistributedRelation) error {
	if err := pi.WriteHeader("attach table"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, r := range ds {
		if err := pi.WriteDataRow(fmt.Sprintf("attached relation %s to distribution %s", r.Name, id)); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) AlterDistributionDetach(_ context.Context, id string, relName string) error {
	if err := pi.WriteHeader("detach relation"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("detached relation %s from distribution %s", relName, id)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) ReportStmtRoutedToAllShards(ctx context.Context) error {
	if err := pi.WriteHeader("explain query"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow("query routed to all shards (multishard)"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) KillClient(clientID uint) error {
	if err := pi.WriteHeader("kill client"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("the client %d was killed", clientID)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	return pi.CompleteMsg(0)
}

// TODO : unit tests
func (pi *PSQLInteractor) BackendConnections(ctx context.Context, shs []shard.Shardinfo) error {
	if err := pi.WriteHeader("backend connection id", "router", "shard key name", "hostname", "user", "dbname", "sync", "tx_served", "tx status"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, sh := range shs {
		router := "no data"
		s, ok := sh.(shard.CoordShardinfo)
		if ok {
			router = s.Router()
		}

		if err := pi.WriteDataRow(fmt.Sprintf("%d", sh.ID()), router, sh.ShardKeyName(), sh.InstanceHostname(), sh.Usr(), sh.DB(), strconv.FormatInt(sh.Sync(), 10), strconv.FormatInt(sh.TxServed(), 10), sh.TxStatus().String()); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}

	}

	return pi.CompleteMsg(len(shs))
}

// Relations sends information about attached relations that satisfy conditions in WHERE-clause
// TODO unit tests
func (pi *PSQLInteractor) Relations(dsToRels map[string][]*distributions.DistributedRelation, condition spqrparser.WhereClauseNode) error {
	if err := pi.cl.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
		TextOidFD("Relation name"),
		TextOidFD("Distribution ID"),
		TextOidFD("Distribution key"),
	}}); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	dss := make([]string, len(dsToRels))
	i := 0
	for ds := range dsToRels {
		dss[i] = ds
		i++
	}
	sort.Strings(dss)

	c := 0
	index := map[string]int{"distribution_id": 0}
	for _, ds := range dss {
		rels := dsToRels[ds]
		sort.Slice(rels, func(i, j int) bool {
			return rels[i].Name < rels[j].Name
		})
		if ok, err := MatchRow([]string{ds}, index, condition); err != nil {
			return err
		} else if !ok {
			continue
		}
		for _, rel := range rels {
			dsKey := make([]string, len(rel.DistributionKey))
			for i, e := range rel.DistributionKey {
				t, err := hashfunction.HashFunctionByName(e.HashFunction)
				if err != nil {
					return err
				}
				dsKey[i] = fmt.Sprintf("(\"%s\", %s)", e.Column, hashfunction.ToString(t))
			}
			if err := pi.cl.Send(&pgproto3.DataRow{
				Values: [][]byte{
					[]byte(rel.Name),
					[]byte(ds),
					[]byte(strings.Join(dsKey, ",")),
				},
			}); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
				return err
			}
			c++
		}
	}
	return pi.CompleteMsg(c)
}
