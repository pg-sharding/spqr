package shhttp

import (
	context "context"

	"github.com/pg-sharding/spqr/router/pkg"
	shards "github.com/pg-sharding/spqr/router/protos"
)

type Routerserver struct {
	shards.UnimplementedRouterServer

	pkg.RouterConn
}

func (r Routerserver) Process(ctx context.Context, request *shards.QueryExecuteRequest) (*shards.QueryExecuteResponse, error) {
	return r.RouterConn.Process(ctx, request)
}

var _ shards.RouterServer = &Routerserver{}
