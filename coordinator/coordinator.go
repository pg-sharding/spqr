package coordinator

import (
	"github.com/pg-sharding/spqr/pkg/meta"

	"github.com/pg-sharding/spqr/pkg/clientinteractor"
)

type Coordinator interface {
	clientinteractor.Interactor
	meta.EntityMgr
}
