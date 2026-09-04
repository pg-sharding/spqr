package rebootstrap

import (
	"context"
	"sort"

	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"google.golang.org/grpc"
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

func MemQDBReBootstrapGRPC(ctx context.Context, memqdb *qdb.MemQDB, cc *grpc.ClientConn) error {
	swapDb, err := qdb.NewMemQDB("")
	if err != nil {
		return err
	}

	swapDb.State.Shards = memqdb.State.Shards

	dsCl := proto.NewDistributionServiceClient(cc)
	krCl := proto.NewKeyRangeServiceClient(cc)
	dsResp, err := dsCl.ListDistributions(ctx, nil)
	if err != nil {
		return err
	}
	for _, ds := range dsResp.Distributions {
		d, err := distributions.DistributionFromProto(ds)
		if err != nil {
			return err
		}
		dStmts, err := swapDb.CreateDistribution(ctx, distributions.DistributionToDB(d))
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance (prepare phase)")
			return err
		}

		if err := swapDb.ExecNoTransaction(ctx, dStmts); err != nil {
			return err
		}

		/* initialize key ranges within distribution */
		krsResp, err := krCl.ListKeyRange(ctx, &proto.ListKeyRangeRequest{Distribution: d.Id})
		if err != nil {
			return err
		}
		krs := make([]*qdb.KeyRange, len(krsResp.KeyRangesInfo))
		for i := range krsResp.KeyRangesInfo {
			krInt, err := kr.KeyRangeFromProto(krsResp.KeyRangesInfo[i], ds.ColumnTypes)
			if err != nil {
				return err
			}
			krs[i] = krInt.ToDB()
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

	refRelCl := proto.NewReferenceRelationsServiceClient(cc)
	refRelsResp, err := refRelCl.ListReferenceRelations(ctx, nil)
	if err != nil {
		return err
	}

	refRels := make([]*qdb.ReferenceRelation, len(refRelsResp.Relations))
	for i := range refRelsResp.Relations {
		refRels[i] = rrelation.RefRelationToDB(rrelation.RefRelationFromProto(refRelsResp.Relations[i]))
	}

	for _, rr := range refRels {
		if err := swapDb.CreateReferenceRelation(ctx, rr); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance")
			return err
		}
	}

	// TODO: initialize two-phase meta storage
	twoPhaseTxMetaCl := proto.NewTwoPhaseTxMetaServiceClient(cc)
	storageResp, err := twoPhaseTxMetaCl.GetTwoPhaseTxMetaStorage(ctx, nil)
	if err != nil {
		return err
	}
	spqrlog.Zero.Debug().Strs("storage", storageResp.Storage).Msg("got dcs storage from etcd")
	if err := swapDb.SetTxMetaStorage(ctx, storageResp.Storage); err != nil {
		return err
	}

	memqdb.SwapState(swapDb.State)
	return nil
}
