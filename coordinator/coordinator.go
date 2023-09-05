package coordinator

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/meta"
)

type Coordinator interface {
	clientinteractor.Interactor
	meta.EntityMgr

	RunCoordinator(ctx context.Context, initialRouter bool)
}
