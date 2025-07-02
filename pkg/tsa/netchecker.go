package tsa

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

type NetChecker struct {
}

var _ TSAChecker = (*NetChecker)(nil)

// CheckTSA checks the TSA for a given shard and returns the result, comment, and error.
// CheckTSA do not use the cache, it always check the TSA.
//
// Parameters:
//   - sh: The shard to check the TSA for.
//
// Returns:
//   - CheckResult: A struct containing the result of the TSA check.
//   - error: An error if any occurred during the process.
func (NetChecker) CheckTSA(sh shard.Shard) (CheckResult, error) {
	if err := sh.Send(&pgproto3.Query{
		String: "SHOW transaction_read_only",
	}); err != nil {
		spqrlog.Zero.Debug().
			Uint("shard", sh.ID()).
			Err(err).
			Msg("netchecker: failed to send transaction_read_only")
		return CheckResult{
			Alive:  false,
			Reason: "failed to send transaction_read_only",
		}, err
	}

	readOnly := false
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
				RO:     false,
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
			if len(qt.Values) == 1 && len(qt.Values[0]) == 3 && qt.Values[0][0] == 'o' && qt.Values[0][1] == 'f' && qt.Values[0][2] == 'f' {
				readOnly = false
				reason = "primary"
			} else if len(qt.Values) == 1 && len(qt.Values[0]) == 2 && qt.Values[0][0] == 'o' && qt.Values[0][1] == 'n' {
				readOnly = true
				reason = "replica"
			} else {
				spqrlog.Zero.Debug().
					Uint("shard", sh.ID()).
					Msg("netchecker: got unexpected datarow while calculating")
				return CheckResult{
					Alive:  false,
					RO:     false,
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
					RO:     readOnly,
					Reason: "the connection was unsynced while acquiring it",
				}, fmt.Errorf("the connection was unsynced while acquiring it")
			}

			spqrlog.Zero.Debug().
				Uint("shard", sh.ID()).
				Bool("read_only", readOnly).
				Bool("alive", true).
				Msg("netchecker: finished")
			return CheckResult{
				Alive:  true,
				RO:     readOnly,
				Reason: reason,
			}, nil
		}
	}
}
