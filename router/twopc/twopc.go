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
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/server"
)

const (
	COMMIT_STRATEGY_BEST_EFFORT = "best-effort"
	/* same af above */
	COMMIT_STRATEGY_1PC = "1pc"
	COMMIT_STRATEGY_2PC = "2pc"
)

func ExecuteTwoPhaseCommit(q qdb.DCStateKeeper, clid uint, s server.Server) (txstatus.TXStatus, error) {

	/*
	* go along first phase
	 */
	uid7, err := uuid.NewV7()
	if err != nil {
		return txstatus.TXERR, err
	}
	gid := uid7.String()

	/* Store our intentions in state keeper */
	/* XXX: we actaully accept nil as valid DCStateKeeper, so be carefull */
	shs := []string{}

	for _, dsh := range s.Datashards() {
		shs = append(shs, dsh.SHKey().Name)
	}

	if q != nil {
		q.RecordTwoPhaseMembers(gid, shs)
	}

	retST := txstatus.TXERR

	for _, dsh := range s.Datashards() {
		st, err := shard.DeployTxOnShard(dsh, &pgproto3.Query{
			String: fmt.Sprintf(`PREPARE TRANSACTION '%s'`, gid),
		}, txstatus.TXIDLE)

		if err != nil {
			/* assert st == txtstatus.TXERR? */
			return txstatus.TXERR, err
		}

		retST = st
	}

	if config.RouterConfig().EnableICP {
		if err := icp.CheckControlPoint(icp.TwoPhaseDecisionCP); err != nil {
			spqrlog.Zero.Info().Uint("client", clid).Str("txid", gid).Err(err).Msg("error while checking control point")
		}
	}

	/* XXX: we actaully accept nil as valid DCStateKeeper, so be carefull */
	if q != nil {
		q.ChangeTxStatus(gid, qdb.TwoPhaseCommitting)
	}

	spqrlog.Zero.Info().Uint("client", clid).Str("txid", gid).Msg("first phase succeeded")

	for _, dsh := range s.Datashards() {
		st, err := shard.DeployTxOnShard(dsh, &pgproto3.Query{
			String: fmt.Sprintf(`COMMIT PREPARED '%s'`, gid),
		}, txstatus.TXIDLE)

		if err != nil {
			/* assert st == txtstatus.TXERR? */
			/* XXX: We now should discard all connection
			* and let recovery algorithm complete tx */
			return txstatus.TXERR, err
		}

		txst := txstatus.TXStatus(st)
		spqrlog.Zero.Info().Uint("client", clid).Str("status", txst.String()).Str("shard", dsh.ShardKeyName()).Str("txid", gid).Msg("committed on shard")

		retST = txst
	}

	return retST, nil
}
