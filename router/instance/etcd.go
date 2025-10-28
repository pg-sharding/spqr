package instance

import (
	"context"
	"sort"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
)

type EtcdMetadataBootstrapper struct {
	QdbAddr string
}

// InitializeMetadata implements RouterMetadataBootstrapper.
func (e *EtcdMetadataBootstrapper) InitializeMetadata(ctx context.Context, r RouterInstance) error {
	etcdConn, err := qdb.NewEtcdQDB(e.QdbAddr, 0)
	if err != nil {
		return err
	}
	defer func() {
		if err := etcdConn.Client().Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close etcd client")
		}
	}()

	/* Initialize distributions */
	ds, err := etcdConn.ListDistributions(ctx)
	if err != nil {
		return err
	}

	for _, d := range ds {
		if d.ID == distributions.REPLICATED {
			continue
		}
		if err := r.Console().Mgr().CreateDistribution(ctx, distributions.DistributionFromDB(d)); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance")
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

		for _, ckr := range krs {
			kRange, err := kr.KeyRangeFromDB(ckr, d.ColTypes)
			if err != nil {
				return err
			}
			if err := r.Console().Mgr().CreateKeyRange(ctx, kRange); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance")
				return err
			}
		}
	}

	rrels, err := etcdConn.ListReferenceRelations(ctx)
	if err != nil {
		return err
	}

	for _, rr := range rrels {
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
		if err := r.Console().Mgr().CreateReferenceRelation(ctx, rrelation.RefRelationFromDB(rr), entries); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance")
			return err
		}
	}

	retryCnt := 50

	for {
		c, err := etcdConn.GetCoordinator(ctx)
		if err != nil {
			if retryCnt > 0 {
				/* await the router to appear */
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(time.Second):

					retryCnt--
					continue
				}
			}
			return err
		}

		err = r.Console().Mgr().UpdateCoordinator(ctx, c)

		if err == nil {
			break
		}
		return err
	}

	r.Initialize()

	return nil
}

func NewEtcdMetadataBootstrapper(QdbAddr string) RouterMetadataBootstrapper {
	return &EtcdMetadataBootstrapper{QdbAddr: QdbAddr}
}
