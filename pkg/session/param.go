package session

type SessionParamsHolder interface {

	// Get current session DRB
	DefaultRouteBehaviour() string
	SetDefaultRouteBehaviour(local bool, val string)

	SetAutoDistribution(local bool, val string)
	AutoDistribution() string

	SetDistributionKey(local bool, val string)
	DistributionKey() string

	// Get current session distribution
	SetDistribution(local bool, val string)
	Distribution() string

	SetExecuteOn(local bool, val string)
	ExecuteOn() string

	// ShardingKey
	SetShardingKey(local bool, val string)
	ShardingKey() string

	SetAllowMultishard(local bool, val bool)
	AllowMultishard() bool

	SetShowNoticeMsg(val bool)
	ShowNoticeMsg() bool

	SetMaintainParams(val bool)
	MaintainParams() bool

	/* route hint always local */
	SetScatterQuery(val bool)
	ScatterQuery() bool

	BindParams() [][]byte
	SetBindParams([][]byte)

	BindParamFormatCodes() []int16
	SetParamFormatCodes([]int16)
}

const (
	SPQR_DISTRIBUTION            = "__spqr__distribution"
	SPQR_DEFAULT_ROUTE_BEHAVIOUR = "__spqr__default_route_behaviour"
	SPQR_AUTO_DISTRIBUTION       = "__spqr__auto_distribution"
	SPQR_DISTRIBUTION_KEY        = "__spqr__distribution_key"
	SPQR_ALLOW_MULTISHARD        = "__spqr__allow_multishard"
	SPQR_SHARDING_KEY            = "__spqr__sharding_key"
	SPQR_SCATTER_QUERY           = "__spqr__scatter_query"
	SPQR_REPLY_NOTICE            = "__spqr__reply_notice"
	SPQR_MAINTAIN_PARAMS         = "__spqr__maintain_params"
	SPQR_EXECUTE_ON              = "__spqr__execute_on"
)
