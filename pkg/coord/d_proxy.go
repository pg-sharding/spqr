package coord

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/grpccreds"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"google.golang.org/grpc"
)

func DistributedMgr(ctx context.Context, localCoordinator meta.EntityMgr) (meta.EntityMgr, func(), error) {

	if !config.RouterConfig().UseCoordinatorInit && !config.RouterConfig().WithCoordinator {
		return localCoordinator, func() {}, nil
	}

	coordAddr, err := localCoordinator.GetCoordinator(ctx)
	if err != nil {
		return nil, nil, err
	}

	dialOpt, err := grpccreds.DialOption(config.RouterConfig().CoordinatorGrpcTLS)
	if err != nil {
		return nil, nil, fmt.Errorf("init coordinator gRPC TLS for %q: %w", coordAddr, err)
	}

	conn, err := grpc.NewClient(coordAddr, dialOpt)
	if err != nil {
		return nil, nil, err
	}

	return NewAdapter(conn, localCoordinator.GetTxnBatchSize()), func() {
		if err := conn.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
		}
	}, nil
}
