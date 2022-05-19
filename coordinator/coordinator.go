package coordinator

import (
	"context"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"

	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/world"
)

type Coordinator interface {
	clientinteractor.Interactor
	kr.KeyRangeMgr
	shrule.ShardingRulesMgr
	datashards.ShardsManager

	RegisterRouter(ctx context.Context, r *qdb.Router) error
	RegisterWorld(ctx context.Context, w world.World) error

	// deprecated
	AddLocalTable(tname string) error
}
