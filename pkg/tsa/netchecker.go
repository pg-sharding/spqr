package tsa

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

const DefaultTSATimeout = 500 * time.Millisecond

type NetChecker struct {
}

// CheckTSA checks the TSA for a given shard and returns the result, comment, and error.
// CheckTSA do not use the cache, it always check the TSA.
// A nonzero timeout bounds the entire query, including writes. Zero preserves
// any deadline already installed by the caller on a dedicated connection.
//
// Parameters:
//   - sh: The shard to check the TSA for.
//
// Returns:
//   - CheckResult: A struct containing the result of the TSA check.
//   - error: An error if any occurred during the process.
func (NetChecker) CheckTSA(sh shard.ShardHostInstance, timeout time.Duration) (CheckResult, error) {
	if timeout != 0 {
		if err := sh.Instance().Conn().SetDeadline(time.Now().Add(timeout)); err != nil {
			return CheckResult{Reason: err.Error()}, err
		}
		defer func() { _ = sh.Instance().Conn().SetDeadline(time.Time{}) }()
	}

	if err := sh.Send(&pgproto3.Query{
		String: "SHOW transaction_read_only",
	}); err != nil {
		spqrlog.Zero.Debug().
			Uint("shard", sh.ID()).
			Err(err).
			Msg("netchecker: failed to send transaction_read_only")
		return CheckResult{
			Alive:  false,
			RW:     false,
			Reason: "failed to send transaction_read_only",
		}, err
	}

	readWrite := false
	gotRow := false
	reason := "empty"

	for {
		msg, err := sh.Receive()

		if err != nil {
			spqrlog.Zero.Debug().
				Uint("shard", sh.ID()).
				Err(err).
				Msg("netchecker: received an error while receiving the next message")
			return CheckResult{
				Alive:  false,
				RW:     false,
				Reason: "received an error while receiving the next message",
			}, err
		}

		spqrlog.Zero.Debug().
			Uint("shard", sh.ID()).
			Str("type", fmt.Sprintf("%T", msg)).
			Interface("message", msg).
			Msg("netchecker: shard has received the next message")

		switch qt := msg.(type) {
		case *pgproto3.DataRow:
			if gotRow {
				err := fmt.Errorf("multiple rows received for transaction_read_only")
				return CheckResult{Reason: err.Error()}, err
			}
			gotRow = true
			if len(qt.Values) == 1 && len(qt.Values[0]) == 3 && qt.Values[0][0] == 'o' && qt.Values[0][1] == 'f' && qt.Values[0][2] == 'f' {
				readWrite = true
				reason = "primary"
			} else if len(qt.Values) == 1 && len(qt.Values[0]) == 2 && qt.Values[0][0] == 'o' && qt.Values[0][1] == 'n' {
				readWrite = false
				reason = "replica"
			} else {
				spqrlog.Zero.Debug().
					Uint("shard", sh.ID()).
					Msg("netchecker: unexpected datarow while calculating")
				return CheckResult{
					Alive:  false,
					RW:     false,
					Reason: fmt.Sprintf("unexpected datarow received: %v", qt.Values),
				}, fmt.Errorf("unexpected datarow received: %v", qt.Values)
			}

		case *pgproto3.ReadyForQuery:
			if txstatus.TXStatus(qt.TxStatus) != txstatus.TXIDLE {
				spqrlog.Zero.Debug().
					Uint("shard", sh.ID()).
					Msg("netchecker: got unsync connection while calculating")
				return CheckResult{
					Alive:  false,
					RW:     readWrite,
					Reason: "the connection was unsynced while acquiring it",
				}, fmt.Errorf("the connection was unsynced while acquiring it")
			}

			if !gotRow {
				err := fmt.Errorf("no transaction_read_only result received")
				return CheckResult{Reason: err.Error()}, err
			}

			spqrlog.Zero.Debug().
				Uint("shard", sh.ID()).
				Bool("read-write", readWrite).
				Bool("alive", true).
				Msg("netchecker: finished")
			return CheckResult{
				Alive:  true,
				RW:     readWrite,
				Reason: reason,
			}, nil
		case *pgproto3.ErrorResponse:
			err := fmt.Errorf("transaction_read_only: %s (%s)", qt.Message, qt.Code)
			return CheckResult{Reason: err.Error()}, err
		case *pgproto3.RowDescription, *pgproto3.CommandComplete, *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
			// The query result and asynchronous status messages are expected.
		default:
			err := fmt.Errorf("unexpected transaction_read_only message: %T", msg)
			return CheckResult{Reason: err.Error()}, err
		}
	}
}
