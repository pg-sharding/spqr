package session

import "github.com/pg-sharding/spqr/pkg/tsa"

type SessionParamsHolder interface {
	GetTsa() tsa.TSA
	SetTsa(level string, value string)

	Usr() string
	/* XXX: also maybe ROLE support is meaningful? */
	DB() string

	// Get current session DRB
	DefaultRouteBehaviour() string
	SetDefaultRouteBehaviour(level string, val string)

	/* Only statement-level */
	SetAutoDistribution(val string)
	AutoDistribution() string

	/* Only statement-level */
	SetDistributionKey(val string)
	DistributionKey() string

	// Get current session distribution

	SetDistribution(level string, val string)
	Distribution() string

	/*  Only statement level */
	SetDistributedRelation(level string, val string)
	DistributedRelation() string

	SetShardingKey(level string, val string)
	ShardingKey() string

	SetExecuteOn(level string, val string)
	ExecuteOn() string

	SetShowNoticeMsg(level string, val bool)
	ShowNoticeMsg() bool

	/* Statement level makes sence? */
	SetMaintainParams(level string, val bool)
	MaintainParams() bool

	/* route hint always statement-level  */
	SetScatterQuery(val bool)
	ScatterQuery() bool

	/* Check if we apply engine v2 routing for query */
	SetEnhancedMultiShardProcessing(level string, val bool)
	EnhancedMultiShardProcessing() bool

	/* route hint always tx-block-level */
	SetCommitStrategy(value string)
	CommitStrategy() string

	BindParams() [][]byte
	SetBindParams([][]byte)

	BindParamFormatCodes() []int16
	SetParamFormatCodes([]int16)
}

const (
	VirtualParamLevelLocal     = "local"
	VirtualParamLevelStatement = "statement"
	VirtualParamLevelTxBlock   = "txBlock"
)

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
