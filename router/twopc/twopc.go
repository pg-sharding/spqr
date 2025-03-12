package twopc

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/samborkent/uuidv7"
)

const (
	COMMIT_STRATEGY_BEST_EFFORT = "best-effort"
	/* same af above */
	COMMIT_STRATEGY_1PC = "1pc"
	COMMIT_STRATEGY_2PC = "2pc"
)

func ExecuteTwoPhaseCommit(clid uint, s server.Server) error {

	/*
	* go along first phase
	 */

	txid := uuidv7.New().String()

	for _, dsh := range s.Datashards() {
		st, err := shard.DeployTxOnShard(dsh, &pgproto3.Query{
			String: fmt.Sprintf(`PREPARE TRANSACTION '%s'`, txid),
		}, txstatus.TXIDLE)

		if err != nil {
			/* assert st == txtstatus.TXERR? */
			s.SetTxStatus(txstatus.TXStatus(txstatus.TXERR))
			return err
		}

		s.SetTxStatus(txstatus.TXStatus(st))
	}

	spqrlog.Zero.Info().Uint("client", clid).Str("txid", txid).Msg("first phase succeeded")

	for _, dsh := range s.Datashards() {
		st, err := shard.DeployTxOnShard(dsh, &pgproto3.Query{
			String: fmt.Sprintf(`COMMIT PREPARED '%s'`, txid),
		}, txstatus.TXIDLE)

		if err != nil {
			/* assert st == txtstatus.TXERR? */
			s.SetTxStatus(txstatus.TXStatus(txstatus.TXERR))
			return err
		}

		s.SetTxStatus(txstatus.TXStatus(st))
	}

	return nil
}
