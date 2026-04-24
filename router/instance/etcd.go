package instance

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/bootstrap"
)

type EtcdMetadataBootstrapper struct {
	QdbAddrs []string
}

// InitializeMetadata implements RouterMetadataBootstrapper.
// TODO: pack TranEntityManager commands to batches
func (e *EtcdMetadataBootstrapper) InitializeMetadata(ctx context.Context, r RouterInstance) error {

	if err := bootstrap.EtcdReBootstrap(ctx, r.Console().Mgr(), e.QdbAddrs); err != nil {
		return err
	}

	r.Initialize()

	return nil
}

func NewEtcdMetadataBootstrapper(QdbAddrs []string) RouterMetadataBootstrapper {
	return &EtcdMetadataBootstrapper{QdbAddrs: QdbAddrs}
}
