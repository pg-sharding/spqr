package instance

import (
	"context"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
)

type EtcdMetadataBootstraper struct {
	QdbAddr string
}

// InitializeMetadata implements RouterMetadataBootstraper.
func (e *EtcdMetadataBootstraper) InitializeMetadata(ctx context.Context, r RouterInstance) error {
	etcdConn, err := qdb.NewEtcdQDB(e.QdbAddr)
	if err != nil {
		return err
	}
	defer etcdConn.Client().Close()

	/* Initialize distributions */
	ds, err := etcdConn.ListDistributions(ctx)
	if err != nil {
		return err
	}

	for _, d := range ds {
		if err := r.Console().Mgr().CreateDistribution(ctx, distributions.DistributionFromDB(d)); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance")
			return err
		}

		/* initialize key ranges within distribution */
		krs, err := etcdConn.ListKeyRanges(ctx, d.ID)
		if err != nil {
			return err
		}

		for _, ckr := range krs {
			if err := r.Console().Mgr().CreateKeyRange(ctx, kr.KeyRangeFromDB(ckr, d.ColTypes)); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to initialize instance")
				return err
			}
		}
	}

	retryCnt := 50

	for {
		c, err := etcdConn.GetCoordinator(ctx)
		if err != nil {
			if retryCnt > 0 {
				/* await the roiter to appear */
				time.Sleep(time.Second)
				retryCnt--
				continue
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

func NewEtcdMetadataBootstraper(QdbAddr string) RouterMetadataBootstraper {
	return &EtcdMetadataBootstraper{QdbAddr: QdbAddr}
}
