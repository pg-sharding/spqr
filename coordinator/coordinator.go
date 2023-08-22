package coordinator

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/meta"

	"github.com/pg-sharding/spqr/pkg/clientinteractor"
)

type Coordinator interface {
	clientinteractor.Interactor
	meta.EntityMgr

	RunCoordinator(ctx context.Context)
}
