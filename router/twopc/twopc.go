package twopc

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/icp"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/server"
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
	uid7, err := uuid.NewV7()
	if err != nil {
		return err
	}
	txid := uid7.String()

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

	if config.RouterConfig().EnableICP {
		if err := icp.CheckControlPoint(icp.TwoPhaseDecisionCP); err != nil {
			spqrlog.Zero.Info().Uint("client", clid).Str("txid", txid).Err(err).Msg("error while checking control point")
		}
	}

	spqrlog.Zero.Info().Uint("client", clid).Str("txid", txid).Msg("first phase succeeded")

	for _, dsh := range s.Datashards() {
		st, err := shard.DeployTxOnShard(dsh, &pgproto3.Query{
			String: fmt.Sprintf(`COMMIT PREPARED '%s'`, txid),
		}, txstatus.TXIDLE)

		if err != nil {
			/* assert st == txtstatus.TXERR? */
			/* XXX: We now should discard all connection
			* and let recovery algorithm complete tx */
			s.SetTxStatus(txstatus.TXStatus(txstatus.TXERR))
			return err
		}

		s.SetTxStatus(txstatus.TXStatus(st))
	}

	return nil
}
