package coordinator

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/meta"
)

type Coordinator interface {
	meta.EntityMgr

	RunCoordinator(ctx context.Context)
}
