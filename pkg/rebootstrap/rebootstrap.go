package rebootstrap

import (
	"context"
	"sort"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
)

func MemQDBReBootstrap(ctx context.Context, memqdb *qdb.MemQDB, etcdConn *qdb.EtcdQDB) error {
	swapDb, err := qdb.NewMemQDB("")
	if err != nil {
		return err
	}

	swapDb.State.Shards = memqdb.State.Shards

	ds, err := etcdConn.ListDistributions(ctx)
	if err != nil {
		return err
	}
	for _, d := range ds {
		dStmts, err := swapDb.CreateDistribution(ctx, d)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance (prepare phase)")
			return err
		}

		if err := swapDb.ExecNoTransaction(ctx, dStmts); err != nil {
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

		ops := make([]qdb.QdbStatement, 0)
		// TODO: We need to group the key ranges into batches. Executing in batches will improve performance.
		for _, ckr := range krs {
			krStmts, err := swapDb.CreateKeyRange(ctx, ckr)
			if err != nil {
				return err
			}
			ops = append(ops, krStmts...)
		}
		if err := swapDb.ExecNoTransaction(ctx, ops); err != nil {
			return err
		}
	}

	refRels, err := etcdConn.ListReferenceRelations(ctx)
	if err != nil {
		return err
	}

	for _, rr := range refRels {
		if err := swapDb.CreateReferenceRelation(ctx, rr); err != nil {
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
	if err := swapDb.SetTxMetaStorage(ctx, storage); err != nil {
		return err
	}

	memqdb.SwapState(swapDb.State)
	return nil
}
