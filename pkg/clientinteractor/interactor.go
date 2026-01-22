package clientinteractor

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/engine"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/port"
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

	if err := pi.WriteHeader("task_group_id", "destination_shard_id", "source_key_range_id", "destination_key_range_id"); err != nil {
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
	if err := pi.WriteHeader("move_task_id", "temporary_key_range_id", "bound", "state"); err != nil {
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

func (pi *PSQLInteractor) ReferenceRelations(rrs []*rrelation.ReferenceRelation) error {
	vp := engine.ReferenceRelationsScan(rrs)
	return pi.ReplyTTS(vp)
}

// ReplyNotice sends notice message to client
func (pi *PSQLInteractor) ReplyNotice(ctx context.Context, msg string) error {
	return pi.cl.ReplyNotice(msg)
}
