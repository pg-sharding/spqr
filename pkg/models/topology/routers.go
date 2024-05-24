package topology

import (
	"context"

	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type Router struct {
	ID      string
	Address string
	State   qdb.RouterState
}

type RouterMgr interface {
	RegisterRouter(ctx context.Context, r *Router) error
	ListRouters(ctx context.Context) ([]*Router, error)
	UnregisterRouter(ctx context.Context, id string) error
	SyncRouterMetadata(ctx context.Context, router *Router) error
	SyncRouterCoordinatorAddress(ctx context.Context, router *Router) error
	UpdateCoordinator(ctx context.Context, address string) error
	GetCoordinator(ctx context.Context) (string, error)
}

// RouterToProto converts a Router object to a protos.Router object.
// It takes a pointer to a Router object as input and returns a pointer to a protos.Router object.
// The function maps the fields of the Router object to the corresponding fields of the protos.Router object.
// The status field of the Router object is converted to a protos.RouterStatus enum value.
//
// Parameters:
//   - r: A pointer to the Router object to convert.
//
// Returns:
//   - *protos.Router: A pointer to the converted protos.Router object.
func RouterToProto(r *Router) *protos.Router {
	status := 0
	if r.State == qdb.OPENED {
		status = 1
	}
	return &protos.Router{
		Id:      r.ID,
		Address: r.Address,
		Status:  protos.RouterStatus(status),
	}
}

// RouterFromProto converts a protobuf Router object to a Router struct.
// It takes a pointer to a protos.Router object as input and returns a pointer to a Router object.
// The function maps the fields of the protos.Router object to the corresponding fields of the Router object.
// The status field of the protos.Router object is converted to a qdb.RouterState enum value.
//
// Parameters:
//   - r: A pointer to the protos.Router object to convert.
//
// Returns:
//   - *Router: A pointer to the converted Router object.
func RouterFromProto(r *protos.Router) *Router {
	return &Router{
		ID:      r.Id,
		Address: r.Address,
		State:   qdb.RouterState(r.Status.String()),
	}
}

// RouterToDB converts a Router object to a qdb.Router object.
// It creates a new qdb.Router object and copies the ID, Address, and State fields from the input Router object.
// The converted qdb.Router object is then returned.
//
// Parameters:
//   - r: The Router object to convert.
//
// Returns:
//   - *qdb.Router: The converted qdb.Router object.
func RouterToDB(r *Router) *qdb.Router {
	return &qdb.Router{
		ID:      r.ID,
		Address: r.Address,
		State:   r.State,
	}
}
