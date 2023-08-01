package clientinteractor

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/models/dataspaces"
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

func (pi *PSQLInteractor) WriteHeader(stmts ...string) error {
	var desc []pgproto3.FieldDescription
	for _, stmt := range stmts {
		desc = append(desc, TextOidFD(stmt))
	}
	return pi.cl.Send(&pgproto3.RowDescription{Fields: desc})
}

func (pi *PSQLInteractor) WriteDataRow(msgs ...string) error {
	vals := make([][]byte, 0)
	for _, msg := range msgs {
		vals = append(vals, []byte(msg))
	}
	return pi.cl.Send(&pgproto3.DataRow{Values: vals})
}

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

func (pi *PSQLInteractor) Pools(_ context.Context, ps []pool.Pool) error {
	if err := pi.WriteHeader(
		"pool id",
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

func (pi *PSQLInteractor) KeyRanges(krs []*kr.KeyRange) error {
	spqrlog.Zero.Debug().Msg("listing key ranges")

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
			spqrlog.Zero.Error().Err(err).Msg("")
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
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.CommandComplete{},
		&pgproto3.ReadyForQuery{},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return nil
}

func (pi *PSQLInteractor) AddKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {
	if err := pi.WriteHeader("add key range"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("created key range from %s to %s", keyRange.LowerBound, keyRange.UpperBound))}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

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
			return true, fmt.Errorf("not supported logic operation: %s", where.Op)
		}
	case spqrparser.WhereClauseLeaf:
		switch where.Op {
		case "=":
			i, ok := nameToIndex[where.ColRef.ColName]
			if !ok {
				return true, fmt.Errorf("column %s not exists", where.ColRef.ColName)
			}
			return row[i] == where.Value, nil
		default:
			return true, fmt.Errorf("not supported operation %s", where.Op)
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

func (_ ClientDesc) GetRow(cl client.Client, hostname string, rAddr string) []string {
	quantiles := statistics.GetQuantiles()
	rowData := []string{cl.ID(), cl.Usr(), cl.DB(), hostname, rAddr}
	routerStat := statistics.GetClientTimeStatistics(statistics.Router, cl.ID())
	shardStat := statistics.GetClientTimeStatistics(statistics.Shard, cl.ID())
	for _, el := range *quantiles {
		rowData = append(rowData, fmt.Sprintf("%.2fms", routerStat.Quantile(el)))
		rowData = append(rowData, fmt.Sprintf("%.2fms", shardStat.Quantile(el)))
	}
	return rowData
}

func (_ ClientDesc) GetHeader() []string {
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

func (pi *PSQLInteractor) ShardingRules(ctx context.Context, rules []*shrule.ShardingRule) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			TextOidFD("Sharding Rule ID"),
			TextOidFD("Table Name"),
			TextOidFD("Columns"),
			TextOidFD("Hash Function"),
		}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
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
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

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
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return nil
}

func (pi *PSQLInteractor) DropShardingRule(ctx context.Context, id string) error {
	if err := pi.WriteHeader("drop sharding rule"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("dropped sharding rule %s", id)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	if err := pi.WriteHeader("add sharding rule"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("created %s", rule.String())); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	return pi.CompleteMsg(0)
}

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
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("merge key ranges %v and %v", unite.KeyRangeIDLeft, unite.KeyRangeIDRight))}},
		&pgproto3.CommandComplete{},
		&pgproto3.ReadyForQuery{},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
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
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}

	return nil
}

func (pi *PSQLInteractor) Routers(resp []*topology.Router) error {
	if err := pi.WriteHeader("show routers"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range resp {
		if err := pi.WriteDataRow(fmt.Sprintf("router %s-%s", msg.ID, msg.Address)); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

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

func (pi *PSQLInteractor) AddDataspace(ctx context.Context, ks *dataspaces.Dataspace) error {
	if err := pi.WriteHeader("add dataspace"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("created dataspace with id %s", ks.ID())); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	return pi.CompleteMsg(0)
}

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

func (pi *PSQLInteractor) KillClient(clientID string) error {
	if err := pi.WriteHeader("kill client"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("the client %s was killed", clientID)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) BackendConnections(ctx context.Context, shs []shard.Shardinfo) error {
	if err := pi.WriteHeader("backend connection id", "shard key name", "hostname", "user", "dbname", "sync", "tx_served", "tx status"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, sh := range shs {

		if err := pi.WriteDataRow(sh.ID(), sh.ShardKeyName(), sh.InstanceHostname(), sh.Usr(), sh.DB(), strconv.FormatInt(sh.Sync(), 10), strconv.FormatInt(sh.TxServed(), 10), sh.TxStatus().String()); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}

	}

	return pi.CompleteMsg(len(shs))
}
