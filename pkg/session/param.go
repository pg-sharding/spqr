package session

import (
	"github.com/pg-sharding/spqr/router/routehint"
)

type SessionParamsHolder interface {

	// Get current session DRB
	DefaultRouteBehaviour() string
	SetDefaultRouteBehaviour(string)

	// ShardingKey
	ShardingKey() string
	SetShardingKey(string)

	BindParams() [][]byte
	SetBindParams([][]byte)

	BindParamFormatCodes() []int16
	SetParamFormatCodes([]int16)

	ShowNoticeMsg() bool
	SetShowNoticeMsg(bool)

	MaintainParams() bool
	SetMaintainParams(bool)

	RouteHint() routehint.RouteHint
	SetRouteHint(routehint.RouteHint)
}

const (
	SPQR_DISTRIBUTION            = "__spqr__distribution"
	SPQR_DEFAULT_ROUTE_BEHAVIOUR = "__spqr__default_route_behaviour"
	SPQR_SHARDING_KEY            = "__spqr__sharding_key"
	SPQR_SCATTER_QUERY           = "__spqr__scatter_query"
	SPQR_REPLY_NOTICE            = "__spqr__reply_notice"
	SPQR_MAINTAIN_PARAMS         = "__spqr__maintain_params"
)
