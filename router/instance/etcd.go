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
	etcdConn, err := qdb.NewEtcdQDB(e.QdbAddr)
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
			l := kr.KeyRangeFromDB(krs[i], d.ColTypes)
			r := kr.KeyRangeFromDB(krs[j], d.ColTypes)
			return !kr.CmpRangesLess(l.LowerBound, r.LowerBound, d.ColTypes)
		})

		for _, ckr := range krs {
			if err := r.Console().Mgr().CreateKeyRange(ctx, kr.KeyRangeFromDB(ckr, d.ColTypes)); err != nil {
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
		/* XXX: nil for auto inc entry is OK? */
		if err := r.Console().Mgr().CreateReferenceRelation(ctx, rrelation.RefRelationFromDB(rr), nil); err != nil {
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

	r.Open()

	return nil
}

func NewEtcdMetadataBootstrapper(QdbAddr string) RouterMetadataBootstrapper {
	return &EtcdMetadataBootstrapper{QdbAddr: QdbAddr}
}
