package session

import (
	"github.com/pg-sharding/spqr/router/routehint"
)

type SessionParamsHolder interface {
	// get current session dataspace
	Dataspace() string
	SetDataspace(string)
	DataspaceIsDefault() bool

	// Get current session DRB
	DefaultRouteBehaviour() string
	SetDefaultRouteBehaviour(string)

	// ShardingKey
	ShardingKey() string
	SetShardingKey(string)

	BindParams() [][]byte
	SetBindParams([][]byte)

	RouteHint() routehint.RouteHint
	SetRouteHint(routehint.RouteHint)
}

const (
	SPQR_DATASPACE               = "__spqr__dataspace"
	SPQR_DEFAULT_ROUTE_BEHAVIOUR = "__spqr__default_route_behaviour"
	SPQR_SHARDING_KEY            = "__spqr__sharding_key"
	SPQR_SCATTER_QUERY           = "__spqr__scatter_query"
)
