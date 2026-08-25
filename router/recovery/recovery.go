package recovery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/qdb"
)

type TwoPCWatchDog struct {
	d  qdb.XDCStateKeeper
	be []*config.BackendRule
	p  pool.MultiShardTSAPool
}

func NewTwoPCWatchDog(be []*config.BackendRule, tmgr topology.TopologyMgr) (*TwoPCWatchDog, error) {
	if be == nil {
		return nil, fmt.Errorf("invalid watchdog config: nil backend rule")
	}
	wd := &TwoPCWatchDog{
		be: be,
	}

	/* XXX: pass mapping as param here? */
	wd.p =
		pool.NewDBPoolWithDisabledFeatures(tmgr)

	db, err := qdb.GetStateKeeperQDB()
	if err != nil {
		return nil, err
	}
	wd.d = db

	return wd, nil
}

/* Attempt tot recover every not-finished GID.
* On any failure, first encountered error is returned.
 */
func (d *TwoPCWatchDog) RecoverDistributedTx(ctx context.Context) (map[string]struct{}, error) {
	spqrlog.Zero.Info().Msg("enter RecoverDistributedTx")
	shs, err := d.d.ListShards(ctx)
	if err != nil {
		return nil, err
	}

	gids := map[string]struct{}{}

	for _, beRule := range d.be {
		if err := func() error {
			d.p.SetRule(beRule)

			for _, sh := range shs {
				spqrlog.Zero.Info().Str("shard", sh.ID).Str("user", beRule.Usr).Str("db", beRule.DB).Msg("fetching stale two phase commit data")

				serv, err := d.p.ConnectionWithTSA(pool.ConnAllocParams{
					Clid: 0xFFFFFFFFFFFFFFFF,
					Tsa:  tsa.TSA(config.TargetSessionAttrsRW),
				}, kr.ShardKey{
					Name: sh.ID,
				})
				if err != nil {
					return spqrerror.Newf(spqrerror.SPQR_CONNECTION_ERROR, "failed to acquire connection to shard %q for 2pc recovery: %v", sh.ID, err)
				}

				defer func() {
					if err := d.p.Put(serv); err != nil {
						spqrlog.Zero.Debug().Msg("failed to release connection")
					}
				}()

				if err := serv.Instance().Send(&pgproto3.Query{
					String: `
				SELECT gid FROM pg_prepared_xacts;
			`,
				}); err != nil {
					/* Be tidy, return acquired connection. */
					_ = d.p.Discard(serv)
					return err
				}

				/* okay, collect unfinished GID's from this shard */

				if err := func() error {

					for {
						msg, err := serv.Receive()
						if err != nil {
							return err
						}

						switch v := msg.(type) {
						case *pgproto3.ReadyForQuery:
							return nil
						case *pgproto3.DataRow:
							/* process */
							gid := string(v.Values[0])

							spqrlog.Zero.Debug().Str("shard", sh.ID).Str("gid", gid).Msg("found unfinished tx on shard")

							/* XXX: Recheck gid status ? */

							gids[gid] = struct{}{}

						case *pgproto3.CommandComplete:
							/* ok */
						case *pgproto3.RowDescription:
							/* ok */
						default:
							return fmt.Errorf("unexpected msg from server %+v", msg)
						}
					}
				}(); err != nil {
					/* Be tidy, return acquired connection. */
					_ = d.p.Discard(serv)
					return err
				}

			}
			return nil
		}(); err != nil {
			return nil, err
		}
	}

	var recoverErr error
	for gid := range gids {
		/* Try to acquire lock on this GID lifecycle
		* management. We expecting failure here if
		* one of those events happens:
		* 1) TX has alive owner (regular backend running first or second
			phase of 2PC)
		* 2) QDB/DCStateKeeper implementation allow non-single-point-of true (basically,
		* when DCStateKeeper in router-local mem-QDB, not etcd)
		* 3) another recovery routine raced with us and won the race.
		*/
		if err := d.LockAndRecover2PhaseCommitTX(ctx, gid); err != nil {
			spqrlog.Zero.Error().Str("gid", gid).Err(err).Msg("failed to recover unfinished distributed tx")
			if recoverErr == nil {
				recoverErr = err
			}
		}
	}

	if recoverErr != nil {
		return nil, recoverErr
	}

	return gids, nil
}

func (d *TwoPCWatchDog) LockAndRecover2PhaseCommitTX(ctx context.Context, gid string) error {
	recoverCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	acq, err := d.d.AcquireTxOwnership(recoverCtx, gid)
	if err != nil {
		return err
	}
	if acq {
		/* Try to fix things  */
		if err := d.Recover2PhaseCommitTX(recoverCtx, gid); err != nil {
			return spqrerror.Newf(spqrerror.SPQR_TWO_PHASE_ERROR, "failed to recover unfinished distributed tx %q: %v", gid, err)
		}
	}
	return nil
}

func (d *TwoPCWatchDog) CheckTransactionOnShard(serv shard.ShardHostInstance, gid string) (bool, error) {
	if err := serv.Send(&pgproto3.Query{
		String: fmt.Sprintf("SELECT EXISTS(SELECT * FROM pg_prepared_xacts WHERE gid = '%s')", gid),
	}); err != nil {
		return false, err
	}

	res := false

	for {
		msg, err := serv.Receive()
		if err != nil {
			return false, err
		}
		switch v := msg.(type) {
		case *pgproto3.DataRow:
			if len(v.Values) > 0 && v.Values[0][0] == 't' {
				res = true
			}
		case *pgproto3.ReadyForQuery:
			return res, nil
		}
	}
}

func (d *TwoPCWatchDog) FinalizeTxStatus(sh string, gid string, q string) error {
	accErr := make([]string, 0)
	for _, beRule := range d.be {
		if done, err := func() (bool, error) {
			d.p.SetRule(beRule)
			serv, err := d.p.ConnectionWithTSA(pool.ConnAllocParams{
				Clid: 0xFFFFFFFFFFFFFFFF,
				Tsa:  tsa.TSA(config.TargetSessionAttrsRW),
			}, kr.ShardKey{
				Name: sh,
			})
			if err != nil {
				spqrlog.Zero.Error().Err(err).Str("shard", sh).Str("gid", gid).Msg("failed to acquire connection for 2pc finalize")
				return false, spqrerror.Newf(spqrerror.SPQR_CONNECTION_ERROR, "failed to acquire connection to shard %q for 2pc finalize of gid %q: %v", sh, gid, err)
			}

			defer func() {
				if err := d.p.Put(serv); err != nil {
					spqrlog.Zero.Error().Str("gid", gid).Err(err).Msg("failed to release cleanup connection")
				}
			}()

			if res, err := d.CheckTransactionOnShard(serv, gid); err != nil {
				return false, err
			} else if !res {
				/* tx already finalized */
				spqrlog.Zero.Debug().Str("gid", gid).Msg("tx already committed/rejected")
				return true, nil
			}

			/* ROLLBACK/COMMIT */
			if err := d.DeployQueryOnShard(serv, q); err != nil {
				spqrlog.Zero.Debug().Str("gid", gid).Str("user", beRule.Usr).Err(err).Msg("failed to roll back transaction")
				accErr = append(accErr, err.Error())
				return false, nil
			} else {
				return true, nil
			}
		}(); err != nil {
			return err
		} else if done {
			return nil
		}
	}
	return spqrerror.Newf(spqrerror.SPQR_TWO_PHASE_ERROR, "could not finalize two-phase transaction: %s", strings.Join(accErr, ";"))
}

func (d *TwoPCWatchDog) executeCommitShards(shs []string, gid string) error {
	for _, sh := range shs {
		if err := d.FinalizeTxStatus(sh, gid, fmt.Sprintf("COMMIT PREPARED '%s'", gid)); err != nil {
			return err
		}
	}

	return nil
}

func (d *TwoPCWatchDog) executeRollbackShards(shs []string, gid string) error {
	for _, sh := range shs {
		if err := d.FinalizeTxStatus(sh, gid, fmt.Sprintf("ROLLBACK PREPARED '%s'", gid)); err != nil {
			return err
		}
	}

	return nil
}

func (d *TwoPCWatchDog) DeployQueryOnShard(serv shard.ShardHostInstance, s string) error {
	if err := serv.Send(&pgproto3.Query{
		String: s,
	}); err != nil {
		return err
	}
	var deployErr error
	ccReceived := false
	deployErr = nil

	for {
		msg, err := serv.Receive()
		if err != nil {
			return err
		}
		switch v := msg.(type) {
		case *pgproto3.ErrorResponse:
			deployErr = spqrerror.Newf(spqrerror.SPQR_TWO_PHASE_ERROR, "deploy recovery SQL failed: %v", v.Message).Hint(v.Hint).Detail(v.Detail)

		case *pgproto3.CommandComplete:
			ccReceived = true

		case *pgproto3.ReadyForQuery:
			if !ccReceived {
				return spqrerror.New(spqrerror.SPQR_TWO_PHASE_ERROR, "missing command complete message in 2pc recovery")
			}
			return deployErr
		}
	}
}

func (d *TwoPCWatchDog) Recover2PhaseCommitTX(ctx context.Context, gid string) error {
	/* Always be tidy */
	defer func() { _ = d.d.ReleaseTxOwnership(ctx, gid) }()

	status, err := d.d.TXStatus(ctx, gid)
	if err != nil {
		return err
	}
	switch status {
	case qdb.TwoPhaseInitState:
		/* TX owner did not made a decision to commit, rollback  */
		shards, err := d.d.TXCohortShards(ctx, gid)
		if err != nil {
			return err
		}
		if err := d.executeRollbackShards(shards, gid); err != nil {
			return err
		}

		return d.d.ChangeTxStatus(ctx, gid, qdb.TwoPhaseP2Rejected)
	case qdb.TwoPhaseP1:
		shards, err := d.d.TXCohortShards(ctx, gid)
		if err != nil {
			return err
		}
		if err := d.executeCommitShards(shards, gid); err != nil {
			return err
		}
		return d.d.ChangeTxStatus(ctx, gid, qdb.TwoPhaseP2)
	case qdb.TwoPhaseP2:
		return nil
	case qdb.TwoPhaseP2Rejected:
		return fmt.Errorf("unexpected 'rejected' tx status in Recover2PhaseCommitTx, gid \"%s\"", gid)
	default:
		return fmt.Errorf("unexpected 2pc state: %s", status)
	}
}

func (d *TwoPCWatchDog) CleanUpOldTXs(ctx context.Context) ([]string, error) {
	txs, err := d.d.GetTXs(ctx)
	if err != nil {
		return nil, err
	}

	res := make([]string, 0)
	for _, tx := range txs {
		if (tx.State == qdb.TwoPhaseP2 || tx.State == qdb.TwoPhaseP2Rejected) && !tx.UpdatedAt.IsZero() && tx.UpdatedAt.Add(config.RouterConfig().TxDataTTL).Before(time.Now()) {
			res = append(res, tx.Gid)
			if err := d.d.RemoveTXData(ctx, tx.Gid); err != nil {
				return nil, err
			}
		}
	}
	return res, nil
}
