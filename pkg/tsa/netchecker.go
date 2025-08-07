package tsa

import (
	"context"
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
// The check will timeout after DefaultTSATimeout (500ms) and consider the host dead.
//
// Parameters:
//   - sh: The shard to check the TSA for.
//
// Returns:
//   - CheckResult: A struct containing the result of the TSA check.
//   - error: An error if any occurred during the process.
func (NetChecker) CheckTSA(sh shard.ShardHostInstance) (CheckResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTSATimeout)
	defer cancel()

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
	reason := "empty"

	for {
		// Use a goroutine with select to handle both timeout and receive
		messageChan := make(chan pgproto3.BackendMessage, 1)
		errorChan := make(chan error, 1)

		go func() {
			msg, err := sh.Receive()
			if err != nil {
				errorChan <- err
				return
			}
			messageChan <- msg
		}()

		select {
		case <-ctx.Done():
			spqrlog.Zero.Debug().
				Uint("shard", sh.ID()).
				Dur("timeout", DefaultTSATimeout).
				Msg("netchecker: TSA check timed out")
			return CheckResult{
				Alive:  false,
				RW:     false,
				Reason: fmt.Sprintf("TSA check timed out after %v", DefaultTSATimeout),
			}, fmt.Errorf("TSA check timed out after %v", DefaultTSATimeout)
		case err := <-errorChan:
			spqrlog.Zero.Debug().
				Uint("shard", sh.ID()).
				Err(err).
				Msg("netchecker: received an error while receiving the next message")
			return CheckResult{
				Alive:  false,
				RW:     false,
				Reason: "received an error while receiving the next message",
			}, err
		case msg := <-messageChan:
			spqrlog.Zero.Debug().
				Uint("shard", sh.ID()).
				Str("type", fmt.Sprintf("%T", msg)).
				Interface("message", msg).
				Msg("netchecker: shard has received the next message")
			switch qt := msg.(type) {
			case *pgproto3.DataRow:
				if len(qt.Values) == 1 && len(qt.Values[0]) == 3 && qt.Values[0][0] == 'o' && qt.Values[0][1] == 'f' && qt.Values[0][2] == 'f' {
					readWrite = true
					reason = "primary"
				} else if len(qt.Values) == 1 && len(qt.Values[0]) == 2 && qt.Values[0][0] == 'o' && qt.Values[0][1] == 'n' {
					readWrite = false
					reason = "replica"
				} else {
					spqrlog.Zero.Debug().
						Uint("shard", sh.ID()).
						Msg("netchecker: got unexpected datarow while calculating")
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
			}
		}
	}
}
