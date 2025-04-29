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
			Msg("encounter error while sending read-write check")
		return CheckResult{
			RW:     false,
			Reason: "error while sending read-write check",
		}, err
	}

	res := false
	reason := "zero datarow received"

	for {
		msg, err := sh.Receive()
		if err != nil {
			spqrlog.Zero.Debug().
				Uint("shard", sh.ID()).
				Err(err).
				Msg("shard received error during check rw")
			return CheckResult{
				RW:     res,
				Reason: reason,
			}, err
		}

		spqrlog.Zero.Debug().
			Uint("shard", sh.ID()).
			Interface("message", msg).
			Msg("shard received msg during check rw")
		switch qt := msg.(type) {
		case *pgproto3.DataRow:
			spqrlog.Zero.Debug().
				Uint("shard", sh.ID()).
				Interface("datarow", qt).
				Msg("shard checking read-write")
			if len(qt.Values) == 1 && len(qt.Values[0]) == 3 && qt.Values[0][0] == 'o' && qt.Values[0][1] == 'f' && qt.Values[0][2] == 'f' {
				res = true
				reason = "is primary"
			} else {
				reason = fmt.Sprintf("transaction_read_only is %+v", qt.Values)
			}

		case *pgproto3.ReadyForQuery:
			if txstatus.TXStatus(qt.TxStatus) != txstatus.TXIDLE {
				spqrlog.Zero.Debug().
					Uint("shard", sh.ID()).
					Msg("shard got unsync connection while calculating RW")
				return CheckResult{
					RW:     false,
					Reason: reason,
				}, fmt.Errorf("connection unsync while acquiring it")
			}

			spqrlog.Zero.Debug().
				Uint("shard", sh.ID()).
				Bool("result", res).
				Msg("shard calculated RW result")
			return CheckResult{
				RW:     res,
				Reason: reason,
			}, nil
		}
	}
}
