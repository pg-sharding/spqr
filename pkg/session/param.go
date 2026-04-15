package session

import "github.com/pg-sharding/spqr/pkg/tsa"

type BoolGUC interface {
	Get(sph SessionParamsHolder) bool
	Set(sph SessionParamsHolder, level string, val bool)
	Reset()
}

type SessionParamsHolder interface {
	ResolveVirtualBoolParam(name string, defaultVal bool) bool
	RecordVirtualParam(level string, name string, val string)

	GetTsa() tsa.TSA
	SetTsa(level string, value string)
	ResetTsa()

	FindBoolGUC(string) (BoolGUC, error)

	Usr() string
	SetUsr(string)

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

	/* Query routing logic */

	/* route hint always statement-level  */
	SetScatterQuery(val bool)
	ScatterQuery() bool

	/* Check if we apply engine v2 routing for query */
	SetEnhancedMultiShardProcessing(level string, val bool)
	EnhancedMultiShardProcessing() bool

	/*  XXX: developer option */
	SetPreferredEngine(level string, val string)
	PreferredEngine() string

	/* Distributed transactions */

	/* route hint always tx-block-level */
	SetCommitStrategy(value string)
	CommitStrategy() string

	/* Helpers for query binding */

	BindParams() [][]byte
	SetBindParams([][]byte)

	BindParamFormatCodes() []int16
	SetParamFormatCodes([]int16)

	Params() map[string]string
	SetParam(name, value string, isLocal bool)
	StartTx()
	ResetAll()
	Rollback()
	Savepoint(name string)
	CleanupStatementSet()
	ResetParam(name string)
	RollbackToSP(name string)
	CommitActiveSet()

	SetStartupParams(map[string]string)
}

const (
	VirtualParamLevelLocal     = "local"
	VirtualParamLevelStatement = "statement"
	VirtualParamLevelTxBlock   = "txBlock"
)

const (
	SpqrDistribution            = "__spqr__distribution"
	SpqrDistributedRelation    = "__spqr__distributed_relation"
	SpqrDefaultRouteBehaviour = "__spqr__default_route_behaviour"
	SpqrAutoDistribution       = "__spqr__auto_distribution"
	SpqrDistributionKey        = "__spqr__distribution_key"
	SpqrShardingKey            = "__spqr__sharding_key"
	SpqrPreferredEngine        = "__spqr__preferred_engine"
	SpqrCommitStrategy         = "__spqr__commit_strategy"
	SpqrTargetSessionAttrs    = "__spqr__target_session_attrs"
	SpqrExecuteOn              = "__spqr__execute_on"

	/* backward compatibility */
	SpqrTargetSessionAttrsAlias   = "target_session_attrs"
	SpqrTargetSessionAttrsAlias2 = "target-session-attrs"

	/* Boolean */
	SpqrScatterQuery   = "__spqr__scatter_query"
	SpqrReplyNotice    = "__spqr__reply_notice"
	SpqrMaintainParams = "__spqr__maintain_params"
	SpqrEngineV2       = "__spqr__engine_v2"
	/*XXX: should we ever disallow?*/
	SpqrAllowSplitUpdate   = "__spqr__allow_split_update"
	SpqrAllowPostprocessing = "__spqr__allow_postprocessing"
)

func ParamIsBoolean(n string) bool {
	switch n {
	/* SpqrMaintainParams, SpqrReplyNotice SpqrScatterQuery & SpqrEngineV2 are intentionally missed */
	case SpqrAllowSplitUpdate, SpqrAllowPostprocessing:
		return true
	default:
		return false
	}
}
