package recovery

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/qdb"
)

type TwoPCWatchDog struct {
	d  qdb.XQDB
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

	db, err := qdb.GetQDB()
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

		gids := []string{}

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
					gids = append(gids, string(v.Values[0]))
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

		spqrlog.Zero.Debug().Str("shard", sh.ID).Strs("gids", gids).Msg("found unfinished tx on shard")
		if err := d.p.Put(serv); err != nil {
			return err
		}
	}

	return nil
}
