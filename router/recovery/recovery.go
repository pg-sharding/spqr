package recovery

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/qdb"
)

type TwoPCWatchDog struct {
	d  qdb.DCStateKeeper
	be *config.BackendRule
	p  pool.MultiShardTSAPool
}

func NewTwoPCWatchDog(be *config.BackendRule) (*TwoPCWatchDog, error) {
	wd := &TwoPCWatchDog{
		be: be,
	}

	/* XXX: pass mapping as param here? */
	wd.p =
		pool.NewDBPoolWithDisabledFeatures(config.RouterConfig().ShardMapping)

	wd.p.SetRule(wd.be)

	db, err := qdb.GetMemQDB()
	if err != nil {
		return nil, err
	}
	wd.d = db

	return wd, nil
}

func (d *TwoPCWatchDog) RecoverDistributedTx() error {
	ctx := context.TODO()

	shs, err := d.d.ListShards(ctx)
	if err != nil {
		return err
	}

	gids := []string{}

	for _, sh := range shs {
		spqrlog.Zero.Info().Str("shard", sh.ID).Msg("fetching stale two phase commit data")

		serv, err := d.p.ConnectionWithTSA(0xFFFFFFFFFFFFFFFF, kr.ShardKey{
			Name: sh.ID,
		}, tsa.TSA(config.TargetSessionAttrsAny))
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}

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

					gids = append(gids, gid)

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

		if err := d.p.Put(serv); err != nil {
			return err
		}
	}

	for _, gid := range gids {
		/* Try to acquire lock on this GID lifecycle
		* management. We expecting failure here if
		* one of those events happends:
		* 1) TX has alive owner (regular backend running first or second
			phase of 2PC)
		* 2) QDB/DCStateKepper implementation allow non-single-point-of true (basically,
		* when DCStateKepper in router-local mem-QDB, not etcd)
		* 3) another recovery routine raced with us and won the race.
		*/

		if d.d.AcquireTxOwnership(gid) {
			/* Try to fix things  */
			if err := d.Recover2PCTX(gid); err != nil {
				spqrlog.Zero.Debug().Str("gid", gid).Err(err).Msg("error recovering unfinished tx")
			}
		}
	}

	return nil
}

func (d *TwoPCWatchDog) CheckTXOnShards(serv shard.ShardHostInstance, gid string) (bool, error) {
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
			if v.Values[0][0] == 't' {
				res = true
			}
		case *pgproto3.ReadyForQuery:
			return res, nil
		}
	}
}

func (d *TwoPCWatchDog) executeCommitShards(shs []string, gid string) error {
	for _, sh := range shs {
		serv, err := d.p.ConnectionWithTSA(0xFFFFFFFFFFFFFFFF, kr.ShardKey{
			Name: sh,
		}, tsa.TSA(config.TargetSessionAttrsAny))
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
		if res, err := d.CheckTXOnShards(serv, gid); err != nil {
			return err
		} else if !res {
			/* tx already committed */
			continue
		}

		/* Commit */
		if err := d.DeployQueryOnShard(serv, fmt.Sprintf("COMMIT PREPARED '%s'", gid)); err != nil {
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

	for {
		msg, err := serv.Receive()
		if err != nil {
			return err
		}
		switch msg.(type) {
		case *pgproto3.ReadyForQuery:
			return nil
		}
	}
}

func (d *TwoPCWatchDog) executeRollbackShards(shs []string, gid string) error {
	for _, sh := range shs {
		serv, err := d.p.ConnectionWithTSA(0xFFFFFFFFFFFFFFFF, kr.ShardKey{
			Name: sh,
		}, tsa.TSA(config.TargetSessionAttrsAny))
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
		if res, err := d.CheckTXOnShards(serv, gid); err != nil {
			return err
		} else if !res {
			/* tx already committed */
			continue
		}

		/* Commit */
		if err := d.DeployQueryOnShard(serv, fmt.Sprintf("ROLLBACK PREPARED '%s'", gid)); err != nil {
			return err
		}
	}

	return nil
}

func (d *TwoPCWatchDog) Recover2PCTX(gid string) error {
	/* Always be tidy */
	defer d.d.ReleaseTxOwnership(gid)

	status := d.d.TXStatus(gid)
	switch status {
	case qdb.TwoPhaseInitState:
		/* TX owner did not made a decision to commit, rollback  */

		if err := d.executeRollbackShards(d.d.TXCohortShards(gid), gid); err != nil {
			return err
		}

		return d.d.ChangeTxStatus(gid, qdb.TwoPhaseP2Rejected)
	case qdb.TwoPhaseP1:
		if err := d.executeCommitShards(d.d.TXCohortShards(gid), gid); err != nil {
			return err
		}
		return d.d.ChangeTxStatus(gid, qdb.TwoPhaseP2)
	default:
		return fmt.Errorf("unexpected 2pc state: %s", status)
	}
}
