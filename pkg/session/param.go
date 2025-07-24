package session

import "github.com/pg-sharding/spqr/pkg/tsa"

type SessionParamsHolder interface {
	GetTsa() tsa.TSA
	SetTsa(statement bool, value string)

	Usr() string
	/* XXX: also maybe ROLE support is meaningful? */
	DB() string

	// Get current session DRB
	DefaultRouteBehaviour() string
	SetDefaultRouteBehaviour(statement bool, val string)

	SetAutoDistribution(statement bool, val string)
	AutoDistribution() string

	SetDistributionKey(statement bool, val string)
	DistributionKey() string

	// Get current session distribution

	SetDistribution(statement bool, val string)
	Distribution() string

	SetDistributedRelation(statement bool, val string)
	DistributedRelation() string

	SetExecuteOn(statement bool, val string)
	ExecuteOn() string

	// ShardingKey
	SetShardingKey(statement bool, val string)
	ShardingKey() string

	SetShowNoticeMsg(val bool)
	ShowNoticeMsg() bool

	SetMaintainParams(val bool)
	MaintainParams() bool

	/* route hint always local */
	SetScatterQuery(val bool)
	ScatterQuery() bool

	/* Check if we apply engine v2 routing for query */
	SetEnhancedMultiShardProcessing(statement bool, val bool)
	EnhancedMultiShardProcessing() bool

	SetCommitStrategy(bool, string)
	CommitStrategy() string

	BindParams() [][]byte
	SetBindParams([][]byte)

	BindParamFormatCodes() []int16
	SetParamFormatCodes([]int16)
}

const (
	SPQR_DISTRIBUTION            = "__spqr__distribution"
	SPQR_DISTRIBUTED_RELATION    = "__spqr__distributed_relation"
	SPQR_DEFAULT_ROUTE_BEHAVIOUR = "__spqr__default_route_behaviour"
	SPQR_AUTO_DISTRIBUTION       = "__spqr__auto_distribution"
	SPQR_DISTRIBUTION_KEY        = "__spqr__distribution_key"
	SPQR_SHARDING_KEY            = "__spqr__sharding_key"
	SPQR_SCATTER_QUERY           = "__spqr__scatter_query"
	SPQR_REPLY_NOTICE            = "__spqr__reply_notice"
	SPQR_MAINTAIN_PARAMS         = "__spqr__maintain_params"
	SPQR_EXECUTE_ON              = "__spqr__execute_on"
	SPQR_ENGINE_V2               = "__spqr__engine_v2"
	SPQR_COMMIT_STRATEGY         = "__spqr__commit_strategy"
	SPQR_TARGET_SESSION_ATTRS    = "__spqr__target_session_attrs"

	/* backward compatibility */
	SPQR_TARGET_SESSION_ATTRS_ALIAS   = "target_session_attrs"
	SPQR_TARGET_SESSION_ATTRS_ALIAS_2 = "target-session-attrs"
)
