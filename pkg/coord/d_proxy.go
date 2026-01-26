package coord

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func DistributedMgr(ctx context.Context, localCoordinator meta.EntityMgr) (meta.EntityMgr, func(), error) {

	if !config.RouterConfig().UseCoordinatorInit && !config.RouterConfig().WithCoordinator {
		return localCoordinator, func() {}, nil
	}

	coordAddr, err := localCoordinator.GetCoordinator(ctx)
	if err != nil {
		return nil, nil, err
	}

	conn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}

	return NewAdapter(conn), func() {
		if err := conn.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
		}
	}, nil
}
