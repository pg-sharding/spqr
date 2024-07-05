package clientinteractor

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/pg-sharding/spqr/pkg/models/tasks"

	"github.com/pg-sharding/spqr/pkg/models/hashfunction"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/statistics"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
)

type Interactor interface {
	ProcClient(ctx context.Context, nconn net.Conn, pt port.RouterPortType) error
}

type PSQLInteractor struct {
	cl client.Client
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

// TEXTOID https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat#L81
const TEXTOID = 25

// TODO : unit tests

// TextOidFD generates a pgproto3.FieldDescription object with the provided statement text.
//
// Parameters:
// - stmt (string): The statement text to use in the FieldDescription.
//
// Returns:
// - A pgproto3.FieldDescription object initialized with the provided statement text and default values.
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
		desc = append(desc, TextOidFD(stmt))
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

// TODO : unit tests

// AddShard sends the row description message for adding a data shard, followed by a data row
// indicating the creation of the specified data shard, and completes the message.
//
// Parameters:
// - shard (*datashards.DataShard): The datashards.DataShard object to be added.
//
// Returns:
//   - error: An error if sending the messages fails, otherwise nil.
func (pi *PSQLInteractor) AddShard(shard *datashards.DataShard) error {
	if err := pi.WriteHeader("add shard"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("shard id -> %s", shard.ID))}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests

// DropShard sends the row description message for dropping a shard, followed by a data row
// indicating the dropping of the specified shard, and completes the message.
//
// Parameters:
// - id (string): The ID of the shard to be dropped (string).
//
// Returns:
//   - error: An error if sending the messages fails, otherwise nil.
func (pi *PSQLInteractor) DropShard(id string) error {
	if err := pi.WriteHeader("drop shard"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("shard id -> %s", id))}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests

// KeyRanges sends the row description message for key ranges, followed by data rows
// containing details of each key range, and completes the message.
//
// Parameters:
// - krs ([]*kr.KeyRange): The list of *kr.KeyRange objects containing the key range information.
//
// Returns:
//   - error: An error if sending the messages fails, otherwise nil.
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

// CreateKeyRange sends the row description message for adding a key range, followed by a data row
// indicating the creation of the specified key range, and completes the message.
//
// Parameters:
// - ctx (context.Context): The context parameter.
// - keyRange (*kr.KeyRange): The *kr.KeyRange object to be created.
//
// Returns:
//   - error: An error if sending the messages fails, otherwise nil.
func (pi *PSQLInteractor) CreateKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {
	if err := pi.WriteHeader("add key range"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("bound -> %s", keyRange.LowerBound))}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
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
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("key range id -> %v", split.SourceID))}},
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("bound        -> %s", string(split.Bound)))}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests

// LockKeyRange sends the row description message for locking a key range with the specified ID,
// followed by a data row indicating the locking of the key range, and completes the message.
//
// Parameters:
// - ctx (context.Context): The context parameter.
// - krid (string): The ID of the key range to be locked (string).
//
// Returns:
//   - error: An error if sending the messages fails, otherwise nil.
func (pi *PSQLInteractor) LockKeyRange(ctx context.Context, krid string) error {
	if err := pi.WriteHeader("lock key range"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{
			[]byte(fmt.Sprintf("key range id -> %v", krid))},
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

// UnlockKeyRange sends the row description message for unlocking a key range with the specified ID,
// followed by a data row indicating the unlocking of the key range, and completes the message.
//
// Parameters:
// - ctx (context.Context): The context parameter.
// - krid (string): The ID of the key range to be unlocked (string).
//
// Returns:
//   - error: An error if sending the messages fails, otherwise nil.
func (pi *PSQLInteractor) UnlockKeyRange(ctx context.Context, krid string) error {
	if err := pi.WriteHeader("unlock key range"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{
			[]byte(
				fmt.Sprintf("key range id -> %v", krid)),
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

// Tasks sends the list of move tasks to the client.
//
// Parameters:
// - ctx (context.Context): The context parameter.
// - ts ([]*tasks.Task): A slice of *tasks.Task objects representing the move tasks.
//
// Returns:
// - error: An error if sending the tasks fails, otherwise nil.
func (pi *PSQLInteractor) Tasks(_ context.Context, ts []*tasks.Task) error {
	spqrlog.Zero.Debug().Msg("listing move tasks")

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			TextOidFD("State"),
			TextOidFD("Bound"),
			TextOidFD("Source key range ID"),
			TextOidFD("Destination key range ID"),
		},
		},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	for _, task := range ts {

		if err := pi.cl.Send(&pgproto3.DataRow{
			Values: [][]byte{
				[]byte(tasks.TaskStateToStr(task.State)),
				task.Bound,
				[]byte(task.KrIdFrom),
				[]byte(task.KrIdTo),
			},
		}); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}
	return pi.CompleteMsg(0)
}

// DropTaskGroup drops all tasks in the task group.
//
// Parameters:
// - _ (context.Context): The context parameter.
//
// Returns:
// - error: An error if there was a problem dropping the tasks.
func (pi *PSQLInteractor) DropTaskGroup(_ context.Context) error {
	if err := pi.WriteHeader("drop task group"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte("dropped all tasks")}},
	} {
		if err := pi.cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests

// Shards lists the data shards.
//
// Parameters:
// - ctx (context.Context): The context parameter.
// - shards ([]*datashards.DataShard): The list of data shards to be listed.
//
// Returns:
// - error: An error if there was a problem listing the data shards.
func (pi *PSQLInteractor) Shards(ctx context.Context, shards []*datashards.DataShard) error {
	if err := pi.WriteHeader("listing data shards"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	spqrlog.Zero.Debug().Msg("listing shards")

	for _, shard := range shards {
		if err := pi.cl.Send(&pgproto3.DataRow{
			Values: [][]byte{[]byte(fmt.Sprintf("shard id -> %s", shard.ID))},
		}); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests

// MatchRow checks if a row matches a given condition in a WHERE clause.
//
// Parameters:
// - row ([]string): The row of data to be checked.
// - nameToIndex (map[string]int): A map that maps column names to their respective indices in the row.
// - condition (spqrparser.WhereClauseNode): The condition to be checked against the row.
//
// Returns:
// - bool: True if the row matches the condition, false otherwise.
// - error: An error if there was a problem evaluating the condition.
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
		rowData = append(rowData, fmt.Sprintf("%.2fms", statistics.GetTimeQuantile(statistics.Router, el, cl.ID())))
		rowData = append(rowData, fmt.Sprintf("%.2fms", statistics.GetTimeQuantile(statistics.Shard, el, cl.ID())))
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

// Distributions sends distribution data to the PSQL client.
//
// Parameters:
// - _ (context.Context): The context for the operation.
// - distributions ([]*distributions.Distribution): The list of distribution data to send.
//
// Returns:
// - error: An error if any occurred during the operation.
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

// MoveKeyRange moves a key range to a specific shard in the PSQL client.
//
// Parameters:
// - _ (context.Context): The context for the operation.
// - move (*kr.MoveKeyRange): The key range and shard information for the move operation.
//
// Returns:
// - error: An error if any occurred during the operation.
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

// UnregisterRouter unregisters a router with the specified ID.
//
// Parameters:
// - id (string): The ID of the router to unregister.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) UnregisterRouter(id string) error {
	if err := pi.WriteHeader("unregister router"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("router id -> %s", id)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests

// RegisterRouter registers a router with the specified ID and address.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - id (string): The ID of the router to register.
// - addr (string): The address of the router to register.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) RegisterRouter(ctx context.Context, id string, addr string) error {
	if err := pi.WriteHeader("register router"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("router -> %s-%s", id, addr)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests

// StartTraceMessages initiates tracing of messages in the PSQL client.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
//
// Returns:
// - error: An error if any occurred during the operation.
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

// StopTraceMessages stops tracing of messages in the PSQL client.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
//
// Returns:
// - error: An error if any occurred during the operation.
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

// StopTraceMessages stops tracing of messages in the PSQL client.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) DropKeyRange(ctx context.Context, ids []string) error {
	if err := pi.WriteHeader("drop key range"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, id := range ids {
		if err := pi.WriteDataRow(fmt.Sprintf("key range id -> %s", id)); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	return pi.CompleteMsg(0)
}

// TODO : unit tests

// AddDistribution adds a distribution to the PSQL client.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - ks (*distributions.Distribution): The distribution to add.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) AddDistribution(ctx context.Context, ks *distributions.Distribution) error {
	if err := pi.WriteHeader("add distribution"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if err := pi.WriteDataRow(fmt.Sprintf("distribution id -> %s", ks.ID())); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}
	return pi.CompleteMsg(0)
}

// TODO : unit tests

// DropDistribution drops distributions with the specified IDs in the PSQL client.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - ids ([]string): The list of distribution IDs to drop.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) DropDistribution(ctx context.Context, ids []string) error {
	if err := pi.WriteHeader("drop distribution"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, id := range ids {
		if err := pi.WriteDataRow(fmt.Sprintf("distribution id -> %s", id)); err != nil {
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
		if err := pi.WriteDataRow(fmt.Sprintf("relation name   -> %s", r.Name)); err != nil {
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

// TODO : unit tests

// BackendConnections writes backend connection information to the PSQL client.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - shs ([]shard.Shardinfo): The list of shard information.
//
// Returns:
// - error: An error if any occurred during the operation.
func (pi *PSQLInteractor) BackendConnections(ctx context.Context, shs []shard.Shardinfo) error {
	if err := pi.WriteHeader("backend connection id", "router", "shard key name", "hostname", "pid", "user", "dbname", "sync", "tx_served", "tx status"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	for _, sh := range shs {
		router := "no data"
		s, ok := sh.(shard.CoordShardinfo)
		if ok {
			router = s.Router()
		}

		if err := pi.WriteDataRow(fmt.Sprintf("%d", sh.ID()), router, sh.ShardKeyName(), sh.InstanceHostname(), fmt.Sprintf("%d", sh.Pid()), sh.Usr(), sh.DB(), strconv.FormatInt(sh.Sync(), 10), strconv.FormatInt(sh.TxServed(), 10), sh.TxStatus().String()); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}

	}

	return pi.CompleteMsg(len(shs))
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
