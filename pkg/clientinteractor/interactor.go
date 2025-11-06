package clientinteractor

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/engine"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/statistics"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type toString[T any] func(s T) string
type BackendGetter func(sh shard.ShardHostCtl) string

func GetRouter(sh shard.ShardHostCtl) string {
	router := "no data"
	s, ok := sh.(shard.CoordShardinfo)
	if ok {
		router = s.Router()
	}
	return router
}

var BackendConnectionsHeaders = []string{
	"backend connection id",
	"router",
	"shard key name",
	"hostname",
	"pid",
	"user",
	"dbname",
	"sync",
	"tx_served",
	"tx status",
	"is stale",
	"created at",
}

var BackendConnectionsGetters = map[string]toString[shard.ShardHostCtl]{
	BackendConnectionsHeaders[0]:  func(sh shard.ShardHostCtl) string { return fmt.Sprintf("%d", sh.ID()) },
	BackendConnectionsHeaders[1]:  GetRouter,
	BackendConnectionsHeaders[2]:  func(sh shard.ShardHostCtl) string { return sh.ShardKeyName() },
	BackendConnectionsHeaders[3]:  func(sh shard.ShardHostCtl) string { return sh.InstanceHostname() },
	BackendConnectionsHeaders[4]:  func(sh shard.ShardHostCtl) string { return fmt.Sprintf("%d", sh.Pid()) },
	BackendConnectionsHeaders[5]:  func(sh shard.ShardHostCtl) string { return sh.Usr() },
	BackendConnectionsHeaders[6]:  func(sh shard.ShardHostCtl) string { return sh.DB() },
	BackendConnectionsHeaders[7]:  func(sh shard.ShardHostCtl) string { return strconv.FormatInt(sh.Sync(), 10) },
	BackendConnectionsHeaders[8]:  func(sh shard.ShardHostCtl) string { return strconv.FormatInt(sh.TxServed(), 10) },
	BackendConnectionsHeaders[9]:  func(sh shard.ShardHostCtl) string { return sh.TxStatus().String() },
	BackendConnectionsHeaders[10]: func(sh shard.ShardHostCtl) string { return strconv.FormatBool(sh.IsStale()) },
	BackendConnectionsHeaders[11]: func(s shard.ShardHostCtl) string { return s.CreatedAt().UTC().Format(time.RFC3339) },
}

type Interactor interface {
	ProcClient(ctx context.Context, nconn net.Conn, pt port.RouterPortType) error
}

type SimpleResultRow struct {
	Name  string
	Value string
}
type SimpleResultMsg struct {
	Header string
	Rows   []SimpleResultRow
}

type PSQLInteractor struct {
	cl client.Client
}

func (pi *PSQLInteractor) CoordinatorAddr(ctx context.Context, addr string) error {
	if err := pi.WriteHeader("coordinator address"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	if err := pi.WriteDataRow(
		fmt.Sprintf("%v", addr)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	return pi.CompleteMsg(1)
}

// TODO refactor it to make more user-friendly
func (pi *PSQLInteractor) Instance(ctx context.Context, ci connmgr.ConnectionMgr) error {
	if err := pi.WriteHeader(
		"total tcp connection count",
		"total cancel requests",
		"active tcp connections"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	if err := pi.WriteDataRow(
		fmt.Sprintf("%v", ci.TotalTcpCount()),
		fmt.Sprintf("%v", ci.TotalCancelCount()),
		fmt.Sprintf("%v", ci.ActiveTcpCount())); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	return pi.CompleteMsg(1)
}

// NewPSQLInteractor creates a new instance of the PSQLInteractor struct.
//
// Parameters:
// - cl (client.Client): The client.Client object to be associated with the PSQLInteractor.
//
// Returns:
// - A pointer to the newly created PSQLInteractor object.
func NewPSQLInteractor(cl client.Client) *PSQLInteractor {
	return &PSQLInteractor{
		cl: cl,
	}
}

// TODO : unit tests

// CompleteMsg sends the completion message with the specified row count.
//
// Parameters:
// - rowCnt (int): The row count to include in the completion message.
//
// Returns:
//   - error: An error if sending the message fails, otherwise nil.
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

// TODO : unit tests

// WriteHeader sends the row description message with the specified field descriptions.
//
// Parameters:
// - stmts ([]string): The list of statement texts to use as field names in the RowDescription message.
//
// Returns:
//   - error: An error if sending the message fails, otherwise nil.
func (pi *PSQLInteractor) WriteHeader(stmts ...string) error {
	var desc []pgproto3.FieldDescription
	for _, stmt := range stmts {
		desc = append(desc, engine.TextOidFD(stmt))
	}
	return pi.cl.Send(&pgproto3.RowDescription{Fields: desc})
}

// TODO : unit tests

// WriteDataRow sends the data row message with the specified values.
//
// Parameters:
// - msgs ([]string): The list of string values to include in the DataRow message.
//
// Returns:
//   - error: An error if sending the message fails, otherwise nil.
func (pi *PSQLInteractor) WriteDataRow(msgs ...string) error {
	vals := make([][]byte, 0)
	for _, msg := range msgs {
		vals = append(vals, []byte(msg))
	}
	return pi.cl.Send(&pgproto3.DataRow{Values: vals})
}

// TODO : unit tests

// Databases sends the row description message for the "show databases" statement,
// followed by data rows containing the provided database names, and finally completes
// the message with the number of rows sent.
//
// Parameters:
// - dbs ([]string): The list of database names to include in the data rows.
//
// Returns:
//   - error: An error if sending the messages fails, otherwise nil.
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

// Pools sends the row description message for pool information, followed by data rows
// containing details of each pool, and completes the message with the number of pools sent.
//
// Parameters:
// - _ (context.Context): The context parameter (not used in the function).
// - ps ([]pool.Pool): The list of pool.Pool objects containing the pool information.
//
// Returns:
//   - error: An error if sending the messages fails, otherwise nil.
func (pi *PSQLInteractor) Pools(_ context.Context, ps []pool.Pool) error {
	if err := pi.WriteHeader(
		"pool id",
		"pool router",
		"pool db",
		"pool usr",
		"pool host",
		"used connections",
		"idle connections",
		"queue residual size"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	for _, p := range ps {
		statistics := p.View()
		if err := pi.WriteDataRow(
			fmt.Sprintf("%p", p),
			statistics.RouterName,
			statistics.DB,
			statistics.Usr,
			statistics.Hostname,
			fmt.Sprintf("%d", statistics.UsedConnections),
			fmt.Sprintf("%d", statistics.IdleConnections),
			fmt.Sprintf("%d", statistics.QueueResidualSize)); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(len(ps))
}

// TODO : unit tests

// Version sends the row description message for the SPQR version and a data row with the SPQR version revision.
//
// Parameters:
// - _ (context.Context): The context parameter (not used in the function).
//
// Returns:
//   - error: An error if sending the messages fails, otherwise nil.
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

// Quantiles sends the row description message for total time quantiles of queries in router and in shard.
//
// Parameters:
// - _ (context.Context): The context parameter (not used in the function).
//
// Returns:
//   - error: An error if sending the messages fails, otherwise nil.
//
// TODO: unit tests
func (pi *PSQLInteractor) Quantiles(_ context.Context) error {
	if err := pi.cl.Send(&pgproto3.RowDescription{
		Fields: []pgproto3.FieldDescription{engine.TextOidFD("quantile_type"), engine.FloatOidFD("time, ms")},
	}); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Could not write header for time quantiles")
		return err
	}

	quantiles := statistics.GetQuantiles()
	quantilesStr := statistics.GetQuantilesStr()
	spqrlog.Zero.Debug().Str("quantiles", fmt.Sprintf("%#v", quantiles)).Msg("Got quantiles")
	if len(*quantiles) != len(*quantilesStr) {
		return fmt.Errorf("malformed configuration for quantilesStr")
	}

	for i := range *quantiles {
		q := (*quantiles)[i]
		qStr := (*quantilesStr)[i]
		if qStr == "" {
			qStr = fmt.Sprintf("%.2f", q)
		}
		if err := pi.WriteDataRow(fmt.Sprintf("router_time_%s", qStr), fmt.Sprintf("%.2f", statistics.GetTotalTimeQuantile(statistics.StatisticsTypeRouter, q))); err != nil {
			return err
		}
		if err := pi.WriteDataRow(fmt.Sprintf("shard_time_%s", qStr), fmt.Sprintf("%.2f", statistics.GetTotalTimeQuantile(statistics.StatisticsTypeShard, q))); err != nil {
			return err
		}
	}

	return pi.CompleteMsg(len(*quantiles) * 2)
}

// TODO : unit tests

func (pi *PSQLInteractor) ReplyTTS(tts *tupleslot.TupleTableSlot) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: tts.Desc},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	for _, r := range tts.Raw {
		if err := pi.cl.Send(&pgproto3.DataRow{
			Values: r,
		}); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}
	return pi.CompleteMsg(len(tts.Raw))
}

// TODO : unit tests

// SplitKeyRange sends the row description message for splitting a key range, followed by a data row
// indicating the split of the key range, and completes the message.
//
// Parameters:
// - ctx (context.Context): The context parameter.
// - split (*kr.SplitKeyRange): The *kr.SplitKeyRange object containing information about the split.
//
// Returns:
//   - error: An error if sending the messages fails, otherwise nil.
func (pi *PSQLInteractor) SplitKeyRange(ctx context.Context, split *kr.SplitKeyRange) error {
	if err := pi.WriteHeader("split key range"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{fmt.Appendf(nil, "key range id -> %v", split.Krid)}},
		&pgproto3.DataRow{Values: [][]byte{fmt.Appendf(nil, "bound        -> %s", strings.ToLower(string(split.Bound[0])))}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// MoveTaskGroup sends the list of move tasks to the client.
//
// Parameters:
// - ctx (context.Context): The context parameter.
// - ts ([]*tasks.MoveTask): A slice of *tasks.MoveTask objects representing the move tasks.
//
// Returns:
// - error: An error if sending the tasks fails, otherwise nil.
func (pi *PSQLInteractor) MoveTaskGroups(_ context.Context, groups map[string]*tasks.MoveTaskGroup) error {
	spqrlog.Zero.Debug().Msg("show move task group")

	if err := pi.WriteHeader("Task group ID", "Destination shard ID", "Source key range ID", "Destination key range ID"); err != nil {
		return err
	}
	if groups == nil {
		return pi.CompleteMsg(0)
	}
	for _, ts := range groups {
		if err := pi.WriteDataRow(ts.ID, ts.ShardToId, ts.KrIdFrom, ts.KrIdTo); err != nil {
			return err
		}
	}
	return pi.CompleteMsg(len(groups))
}

func (pi *PSQLInteractor) MoveTasks(_ context.Context, ts map[string]*tasks.MoveTask, dsIDColTypes map[string][]string, moveTaskDsID map[string]string) error {
	if err := pi.WriteHeader("Move task ID", "Temporary key range ID", "Bound", "State"); err != nil {
		return err
	}

	for _, task := range ts {
		krData := []string{""}
		if task.Bound != nil {
			dsID, ok := moveTaskDsID[task.ID]
			if !ok {
				return fmt.Errorf("failed to reply move task data: distribution for task \"%s\" not found", task.ID)
			}
			moveTaskColTypes, ok := dsIDColTypes[dsID]
			if !ok {
				return fmt.Errorf("failed to reply move task data: column types for distribution \"%s\" not found", dsID)
			}
			if len(task.Bound) != len(moveTaskColTypes) {
				err := fmt.Errorf("something wrong in task: %s, columns: %#v", task.ID, moveTaskColTypes)
				return err
			}
			kRange, err := kr.KeyRangeFromBytes(task.Bound, moveTaskColTypes)
			if err != nil {
				return err
			}
			krData = kRange.SendRaw()
		}
		if err := pi.WriteDataRow(
			task.ID,
			task.KrIdTemp,
			strings.Join(krData, ";"),
			tasks.TaskStateToStr(task.State),
		); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("Failed to send move task data")
			return err
		}

	}
	return pi.CompleteMsg(len(ts))
}

type TableDesc interface {
	GetHeader() []string
}

type ClientDesc struct {
}

// TODO : unit tests

// GetRow retrieves a row of data for a given client, hostname, and rAddr.
//
// Parameters:
// - cl (client.Client): The client object.
// - hostname (string): The hostname.
// - rAddr (string): The rAddr.
//
// Returns:
// - []string: The row data, which consists of the following elements:
//   - ID (int): The ID of the client.
//   - Usr (string): The user of the client.
//   - DB (string): The database of the client.
//   - Hostname (string): The hostname.
//   - RAddr (string): The rAddr.
//   - Quantiles ([]float64): The quantiles of time statistics for the client.
//   - TimeQuantileRouter (float64): The time quantile for the router.
//   - TimeQuantileShard (float64): The time quantile for the shard.
func (ClientDesc) GetRow(cl client.Client, hostname string, rAddr string) []string {
	quantiles := statistics.GetQuantiles()
	rowData := []string{fmt.Sprintf("%d", cl.ID()), cl.Usr(), cl.DB(), hostname, rAddr}

	for _, el := range *quantiles {
		rowData = append(rowData, fmt.Sprintf("%.2fms",
			statistics.GetTimeQuantile(statistics.StatisticsTypeRouter, el, cl)))
		rowData = append(rowData, fmt.Sprintf("%.2fms",
			statistics.GetTimeQuantile(statistics.StatisticsTypeShard, el, cl)))
	}
	return rowData
}

// TODO : unit tests

// GetHeader returns the header row for the client description.
//
// Parameters:
// - None.
//
// Returns:
// - []string: The header row, which consists of the following elements:
//   - "client_id" (string): The ID of the client.
//   - "user" (string): The user of the client.
//   - "dbname" (string): The database of the client.
//   - "server_id" (string): The server ID.
//   - "router_address" (string): The router address.
//   - "router_time_<quantile>" (string): The header for the quantile time for the router.
//   - "shard_time_<quantile>" (string): The header for the quantile time for the shard.
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

type BackendDesc struct {
}

// TODO : unit tests

// GetRow retrieves a row of data for a given client, hostname, and rAddr.
//
// Parameters:
// - cl (client.Client): The client object.
// - hostname (string): The hostname.
// - rAddr (string): The rAddr.
//
// Returns:
// - []string: The row data, which consists of the following elements:
//   - ID (int): The ID of the client.
//   - Usr (string): The user of the client.
//   - DB (string): The database of the client.
//   - Hostname (string): The hostname.
//   - RAddr (string): The rAddr.
//   - Quantiles ([]float64): The quantiles of time statistics for the client.
//   - TimeQuantileRouter (float64): The time quantile for the router.
//   - TimeQuantileShard (float64): The time quantile for the shard.
func (BackendDesc) GetRow(sh shard.ShardHostCtl) []string {
	var rowData []string
	for _, header := range BackendConnectionsHeaders {
		rowData = append(rowData, BackendConnectionsGetters[header](sh))
	}

	return rowData
}

// TODO : unit tests

// GetHeader returns the header row for the client description.
//
// Parameters:
// - None.
//
// Returns:
// - []string: The header row, which consists of the following elements:
//   - "client_id" (string): The ID of the client.
//   - "user" (string): The user of the client.
//   - "dbname" (string): The database of the client.
//   - "server_id" (string): The server ID.
//   - "router_address" (string): The router address.
//   - "router_time_<quantile>" (string): The header for the quantile time for the router.
//   - "shard_time_<quantile>" (string): The header for the quantile time for the shard.
func (BackendDesc) GetHeader() []string {
	return BackendConnectionsHeaders
}

// GetColumnsMap generates a map that maps column names to their respective indices in the table description header.
//
// Parameters:
// - desc (TableDesc): The table description.
//
// Returns:
// - map[string]int: A map that maps column names to their respective indices in the table description header.
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

// Clients retrieves client information based on provided client information, filtering conditions and writes the data to the PSQL client.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - clients ([]client.ClientInfo): The list of client information to process.
// - condition (spqrparser.WhereClauseNode): The condition to filter the client information.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) Clients(ctx context.Context, clients []client.ClientInfo, query *spqrparser.Show) error {
	desc := ClientDesc{}
	header := desc.GetHeader()
	rowDesc := GetColumnsMap(desc)
	condition := query.Where
	order := query.Order
	if err := pi.WriteHeader(header...); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	var data [][]string
	for _, cl := range clients {
		if len(cl.Shards()) > 0 {
			for _, sh := range cl.Shards() {
				if sh == nil {
					continue
				}
				row := desc.GetRow(cl, sh.Instance().Hostname(), cl.RAddr())

				match, err := engine.MatchRow(row, rowDesc, condition)
				if err != nil {
					return err
				}
				if !match {
					continue
				}
				data = append(data, row)
			}
		} else {
			row := desc.GetRow(cl, "no backend connection", cl.RAddr())

			match, err := engine.MatchRow(row, rowDesc, condition)
			if err != nil {
				return err
			}
			if !match {
				continue
			}

			data = append(data, row)
		}

	}
	switch order.(type) {
	case spqrparser.Order:
		ord := order.(spqrparser.Order)
		var asc_desc int

		switch ord.OptAscDesc.(type) {
		case spqrparser.SortByAsc:
			asc_desc = engine.ASC
		case spqrparser.SortByDesc:
			asc_desc = engine.DESC
		case spqrparser.SortByDefault:
			asc_desc = engine.ASC
		default:
			return fmt.Errorf("wrong sorting option (asc/desc)")
		}
		sortable := engine.SortableWithContext{Data: data, Col_index: rowDesc[ord.Col.ColName], Order: asc_desc}
		sort.Sort(sortable)
	}
	for i := range len(data) {
		if err := pi.WriteDataRow(data[i]...); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}
	return pi.CompleteMsg(len(clients))
}

// TODO : unit tests

// Distributions sends distribution data to the PSQL client.
//
// Parameters:
// - _ (context.Context): The context for the operation.
// - distributions ([]*distributions.Distribution): The list of distribution data to send.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) Distributions(_ context.Context, distributions []*distributions.Distribution, defShardIDs []string) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			engine.TextOidFD("Distribution ID"),
			engine.TextOidFD("Column types"),
			engine.TextOidFD("Default shard"),
		}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}
	for id, distribution := range distributions {
		if err := pi.cl.Send(&pgproto3.DataRow{
			Values: [][]byte{
				[]byte(distribution.Id),
				[]byte(strings.Join(distribution.ColTypes, ",")),
				[]byte(defShardIDs[id]),
			},
		}); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}
	return pi.CompleteMsg(0)
}

// TODO : unit tests

// ReportError sends an error response to the PSQL client in case of an error.
//
// Parameters:
// - err (error): The error to report.
//
// Returns:
// - error: An error if any occurred during the operation.
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

// MergeKeyRanges merges two key ranges in the PSQL client.
//
// Parameters:
// - _ (context.Context): The context for the operation.
// - unite (*kr.UniteKeyRange): The key range to merge.
//
// Returns:
// - error: An error if any occurred during the operation.
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
		&pgproto3.DataRow{Values: [][]byte{fmt.Appendf(nil, "merge key ranges %v and %v", unite.BaseKeyRangeId, unite.AppendageKeyRangeId)}},
		&pgproto3.CommandComplete{},
		&pgproto3.ReadyForQuery{},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}

	return nil
}

// RedistributeKeyRange moves key range to a specified shard in the PSQL client.
// Data moves are done by batches of the given size.
//
// Parameters:
// - _ (context.Context): The context for the operation.
// - redistribute (*kr.MoveKeyRange): The key range and shard information for the redistribution operation.
//
// Returns:
// - error: An error if any occurred.
// TODO : unit tests
func (pi *PSQLInteractor) RedistributeKeyRange(_ context.Context, stmt *spqrparser.RedistributeKeyRange) error {
	if err := pi.WriteHeader("redistribute key range"); err != nil {
		return err
	}

	for _, row := range []string{
		fmt.Sprintf("key range id         -> %s", stmt.KeyRangeID),
		fmt.Sprintf("destination shard id -> %s", stmt.DestShardID),
		fmt.Sprintf("batch size           -> %d", stmt.BatchSize),
	} {
		if err := pi.WriteDataRow(row); err != nil {
			return err
		}
	}

	return pi.CompleteMsg(3)
}

// TODO : unit tests

// Routers sends information about routers to the PSQL client.
//
// Parameters:
// - resp ([]*topology.Router): The list of router information to send.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) Routers(resp []*topology.Router) error {
	if err := pi.WriteHeader("show routers", "status"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range resp {
		if err := pi.WriteDataRow(fmt.Sprintf("router -> %s-%s", msg.ID, msg.Address), string(msg.State)); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests

// AlterDistributionAttach attaches tables to a distribution in the PSQL client.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - id (string): The ID of the distribution to attach tables to.
// - ds ([]*distributions.DistributedRelation): The list of distributed relations to attach.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) AlterDistributionAttach(ctx context.Context, id string, ds []*distributions.DistributedRelation) error {
	if err := pi.WriteHeader("attach table"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, r := range ds {
		if err := pi.WriteDataRow(fmt.Sprintf("relation name   -> %s", r.QualifiedName().String())); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}

		if err := pi.WriteDataRow(fmt.Sprintf("distribution id -> %s", id)); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests

// AlterDistributionDetach detaches a relation from a distribution in the PSQL client.
//
// Parameters:
// - _ (context.Context): The context for the operation. (Unused)
// - id (string): The ID of the distribution to detach the relation from.
// - relName (string): The name of the relation to detach.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) AlterDistributionDetach(_ context.Context, id string, relName string) error {
	if err := pi.WriteHeader("detach relation"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("relation name   -> %s", relName)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("distribution id -> %s", id)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	return pi.CompleteMsg(0)
}

// MakeSimpleResponse generic function to return to client result as list of key-value.
//
// Parameters:
// - _ (context.Context): The context for the operation. (Unused)
// - msg (SimpleResultMsg): header and key-value rows
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) MakeSimpleResponse(_ context.Context, msg SimpleResultMsg) error {
	if err := pi.WriteHeader(msg.Header); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	for _, info := range msg.Rows {
		if err := pi.WriteDataRow(fmt.Sprintf("%s	-> %s", info.Name, info.Value)); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}
	return pi.CompleteMsg(len(msg.Rows))
}

// TODO : unit tests

// ReportStmtRoutedToAllShards reports that a statement has been routed to all shards in the PSQL client.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
//
// Returns:
// - error: An error if any occurred during the operation.
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

// KillClient kills a client in the PSQL client.
//
// Parameters:
// - clientID (uint): The ID of the client to kill.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) KillClient(clientID uint) error {
	if err := pi.WriteHeader("kill client"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("client id -> %d", clientID)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	return pi.CompleteMsg(0)
}

/* TODO: pretty-print if specified GUC set */
/* KillBackend reports a backend as killed (marked stale) in the PSQL client. */
func (pi *PSQLInteractor) KillBackend(id uint) error {
	if err := pi.WriteHeader("kill backend"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("backend id -> %d", id)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	return pi.CompleteMsg(0)
}

// BackendConnections writes backend connection information to the PSQL client.
//
// Parameters:
// - _ (context.Context): The context for the operation.
// - shs ([]shard.Shardinfo): The list of shard information.
// - stmt (*spqrparser.Show): The query itself.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) BackendConnections(_ context.Context, shs []shard.ShardHostCtl, stmt *spqrparser.Show) error {

	var filteredShards []shard.ShardHostCtl

	var desc BackendDesc
	rowDesc := GetColumnsMap(desc)

	for _, sh := range shs {

		row := desc.GetRow(sh)

		match, err := engine.MatchRow(row, rowDesc, stmt.Where)
		if err != nil {
			return err
		}
		if !match {
			continue
		}
		filteredShards = append(filteredShards, sh)
	}

	shs = filteredShards

	switch gb := stmt.GroupBy.(type) {
	case spqrparser.GroupBy:
		groupByCols := []string{}
		for _, col := range gb.Col {
			groupByCols = append(groupByCols, col.ColName)
		}
		return groupBy(shs, BackendConnectionsGetters, groupByCols, pi)
	case spqrparser.GroupByClauseEmpty:
		if err := pi.WriteHeader(BackendConnectionsHeaders...); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}

		for _, sh := range shs {
			vals := make([]string, 0)
			for _, header := range BackendConnectionsHeaders {
				vals = append(vals, BackendConnectionsGetters[header](sh))
			}
			if err := pi.WriteDataRow(vals...); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
				return err
			}
		}

		return pi.CompleteMsg(len(shs))
	default:
		return spqrerror.NewByCode(spqrerror.SPQR_INVALID_REQUEST)
	}
}

// TODO unit tests

// Relations sends information about attached relations that satisfy conditions in WHERE-clause
// Relations writes relation information to the PSQL client based on the given distribution-to-relations map and condition.
//
// Parameters:
// - dsToRels (map[string][]*distributions.DistributedRelation): The map of distribution names to their corresponding distributed relations.
// - condition (spqrparser.WhereClauseNode): The condition for filtering the relations.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) Relations(dsToRels map[string][]*distributions.DistributedRelation, condition spqrparser.WhereClauseNode) error {
	if err := pi.WriteHeader("Relation name", "Distribution ID", "Distribution key", "Schema name"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error sending header")
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
		if ok, err := engine.MatchRow([]string{ds}, index, condition); err != nil {
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
			schema := rel.SchemaName
			if schema == "" {
				schema = "$search_path"
			}
			if err := pi.WriteDataRow(rel.Name, ds, strings.Join(dsKey, ","), schema); err != nil {
				return err
			}
			c++
		}
	}
	return pi.CompleteMsg(c)
}

func (pi *PSQLInteractor) ReferenceRelations(rrs []*rrelation.ReferenceRelation) error {
	vp := engine.ReferenceRelationsScan(rrs)
	return pi.ReplyTTS(vp)
}

func (pi *PSQLInteractor) PreparedStatements(ctx context.Context, shs []shard.PreparedStatementsMgrDescriptor) error {
	if err := pi.WriteHeader("name", "backend_id", "hash", "query"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, sh := range shs {
		if err := pi.WriteDataRow(sh.Name, fmt.Sprintf("%d", sh.ServerId), fmt.Sprintf("%d", sh.Hash), sh.Query); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(len(shs))
}

func (pi *PSQLInteractor) Sequences(ctx context.Context, seqs []string, sequenceVals []int64) error {
	if err := pi.WriteHeader("name", "value"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for i, seq := range seqs {
		if err := pi.WriteDataRow(seq, fmt.Sprintf("%d", sequenceVals[i])); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}
	return pi.CompleteMsg(len(seqs))
}

func (pi *PSQLInteractor) IsReadOnly(ctx context.Context, ro bool) error {
	if err := pi.WriteHeader("is read only"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("%v", ro)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	return pi.CompleteMsg(0)
}

func (pi *PSQLInteractor) MoveStats(ctx context.Context, stats map[string]time.Duration) error {
	if err := pi.WriteHeader("statistic", "time"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	for stat, time := range stats {
		if err := pi.WriteDataRow(stat, time.String()); err != nil {
			return err
		}
	}

	return pi.CompleteMsg(len(stats))
}

func (pi *PSQLInteractor) Users(ctx context.Context) error {
	berules := config.RouterConfig().BackendRules
	if err := pi.WriteHeader(
		"user",
		"dbname",
		"connection_limit",
		"connection_retries",
		"connection_timeout",
		"keep_alive",
		"tcp_user_timeout",
	); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	for _, berule := range berules {
		if err := pi.WriteDataRow(
			berule.Usr,
			berule.DB,
			fmt.Sprintf("%d", berule.ConnectionLimit),
			fmt.Sprintf("%d", berule.ConnectionRetries),
			berule.ConnectionTimeout.String(),
			berule.KeepAlive.String(),
			berule.TcpUserTimeout.String(),
		); err != nil {
			return err
		}
	}
	return pi.CompleteMsg(len(berules))
}

// ReplyNotice sends notice message to client
func (pi *PSQLInteractor) ReplyNotice(ctx context.Context, msg string) error {
	return pi.cl.ReplyNotice(msg)
}

// Outputs groupBy get list values and counts its 'groupByCol' property.
// 'groupByCol' sorted in grouped result by string key ASC mode
//
// Parameters:
// - values []T: list of objects for grouping
// - getters (map[string]toString[T]): getters which gets object property as string
// - groupByCol string: property names for counting
// - pi *PSQLInteractor:  output object
// Returns:
// - error: An error if there was a problem dropping the sequence.
func groupBy[T any](values []T, getters map[string]toString[T], groupByCols []string, pi *PSQLInteractor) error {
	groups := make(map[string][]T)
	for _, value := range values {
		key := ""
		for _, groupByCol := range groupByCols {
			if getFun, ok := getters[groupByCol]; ok {
				key = fmt.Sprintf("%s:-:%s", key, getFun(value))
			} else {
				return fmt.Errorf("not found column '%s' for group by statement", groupByCol)
			}
		}
		groups[key] = append(groups[key], value)
	}

	colDescs := make([]pgproto3.FieldDescription, 0, len(groupByCols)+1)
	for _, groupByCol := range groupByCols {
		colDescs = append(colDescs, engine.TextOidFD(groupByCol))
	}
	if err := pi.cl.Send(&pgproto3.RowDescription{
		Fields: append(colDescs, engine.IntOidFD("count")),
	}); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Could not write header for backend connections")
		return err
	}

	keys := make([]string, 0, len(groups))
	for groupKey := range groups {
		keys = append(keys, groupKey)
	}
	sort.Strings(keys)

	for _, key := range keys {
		group := groups[key]
		cols := strings.Split(key, ":-:")[1:]
		if err := pi.WriteDataRow(append(cols, fmt.Sprintf("%d", len(group)))...); err != nil {
			return err
		}
	}
	return pi.CompleteMsg(len(groups))
}
