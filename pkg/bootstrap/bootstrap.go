package bootstrap

import (
	"context"
	"sort"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/rebootstrap"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/sethvargo/go-retry"
)

func EtcdReBootstrap(ctx context.Context, mngr meta.EntityMgr, qdbAddrs []string) error {
	etcdConn, err := qdb.NewEtcdQDB(qdbAddrs, 0)
	if err != nil {
		return err
	}
	defer func() {
		if err := etcdConn.Client().Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close etcd client")
		}
	}()

	/* Initialize shards */
	if config.RouterConfig().ManageShardsByCoordinator {
		shards, err := etcdConn.ListShards(ctx)
		if err != nil {
			return err
		}
		routerShards, err := mngr.ListShards(ctx)
		if err != nil {
			return err
		}
		/* Drop old shards */
		for _, sh := range routerShards {
			if err := mngr.DropShard(ctx, sh.ID); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance")
				return err
			}
		}
		/* Add shards from coordinator */
		for _, sh := range shards {
			if err := mngr.AddDataShard(ctx, topology.DataShardFromDB(sh)); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance")
				return err
			}
		}
	}

	db := mngr.QDB()
	if memqdb, ok := db.(*qdb.MemQDB); ok {
		if err := rebootstrap.MemQDBReBootstrap(ctx, memqdb, etcdConn); err != nil {
			return err
		}
	} else {

		/* Initialize distributions */
		ds, err := etcdConn.ListDistributions(ctx)
		if err != nil {
			return err
		}
		for _, d := range ds {
			if d.ID == distributions.REPLICATED {
				continue
			}
			tranMngr := meta.NewTranEntityManager(mngr)
			if err = tranMngr.CreateDistribution(ctx, distributions.DistributionFromDB(d)); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance (prepare phase)")
				return err
			}
			if err = tranMngr.ExecNoTran(ctx); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance (exec phase)")
				return err
			}

			/* initialize key ranges within distribution */
			krs, err := etcdConn.ListKeyRanges(ctx, d.ID)
			if err != nil {
				return err
			}

			sort.Slice(krs, func(i, j int) bool {
				l, _ := kr.KeyRangeFromDB(krs[i], d.ColTypes)
				r, _ := kr.KeyRangeFromDB(krs[j], d.ColTypes)
				return !kr.CmpRangesLess(l.LowerBound, r.LowerBound, d.ColTypes)
			})
			// TODO: We need to group the key ranges into batches. Executing in batches will improve performance.
			for _, ckr := range krs {
				kRange, err := kr.KeyRangeFromDB(ckr, d.ColTypes)
				if err != nil {
					return err
				}
				tranMngr := meta.NewTranEntityManager(mngr)
				if err := tranMngr.CreateKeyRange(ctx, kRange, d.ColTypes); err != nil {
					spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance")
					return err
				}
				if err = tranMngr.ExecNoTran(ctx); err != nil {
					spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance (exec phase)")
					return err
				}
			}
		}

		refRels, err := etcdConn.ListReferenceRelations(ctx)
		if err != nil {
			return err
		}

		for _, rr := range refRels {
			entries := []*rrelation.AutoIncrementEntry{}

			for c, seq := range rr.ColumnSequenceMapping {
				n, err := etcdConn.CurrVal(ctx, seq)
				if err != nil {
					return err
				}
				entries = append(entries, &rrelation.AutoIncrementEntry{
					Column: c,
					Start:  uint64(n),
				})
			}

			/* XXX: nil for auto inc entry is OK? */
			if err := mngr.CreateReferenceRelation(ctx, rrelation.RefRelationFromDB(rr), entries); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance")
				return err
			}
		}

		// TODO: initialize two-phase meta storage
		storage, err := etcdConn.GetTxMetaStorage(ctx)
		spqrlog.Zero.Debug().Strs("storage", storage).Msg("got dcs storage from etcd")
		if err != nil {
			return err
		}
		if err := mngr.SetTwoPhaseTxMetaStorage(ctx, storage); err != nil {
			return err
		}
	}

	c, err := retry.DoValue(ctx, retry.WithMaxRetries(50, retry.NewConstant(time.Second)), func(ctx context.Context) (string, error) {
		c, err := etcdConn.GetCoordinator(ctx)
		if err != nil {
			return "", retry.RetryableError(err)
		}
		return c, nil
	})
	if err != nil {
		return err
	}
	return mngr.UpdateCoordinator(ctx, c)
}
