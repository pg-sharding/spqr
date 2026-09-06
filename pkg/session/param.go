package session

import (
	"fmt"
	"strings"

	"github.com/pg-sharding/spqr/pkg/tsa"
)

type BoolGUC interface {
	ShortName() string
	Get(sph SessionParamsHolder) bool
	Show(sph SessionParamsHolder) (string, error)
	Set(sph SessionParamsHolder, level string, val bool)
	Reset()
}

type StrGUC interface {
	ShortName() string
	Get(sph SessionParamsHolder) string
	Set(sph SessionParamsHolder, level string, val string) error
	Reset()
}

type SessionParamsHolder interface {
	ResolveVirtualBoolParam(name string, defaultVal bool) bool
	ResolveVirtualStringParam(name string, defaultVal string) string

	RecordVirtualParam(level string, name string, val string)

	GetCatalogSeed() int
	SetSeed(int)

	GetTsa() tsa.TSA
	SetTsa(level string, value string)
	ResetTsa()

	FindBoolGUC(string) (BoolGUC, error)
	FindStrGUC(string) (StrGUC, error)

	Usr() string
	SetUsr(string)

	// Get current session distribution

	SetAutoDistribution(val string)
	AutoDistribution() string

	/* Only statement-level */
	SetDistribution(level string, val string)
	Distribution() string

	/*  Only statement level */
	SetDistributedRelation(level string, val string)
	DistributedRelation() string

	/* Check if we apply engine v2 routing for query */
	SetEnhancedMultiShardProcessing(level string, val bool)
	EnhancedMultiShardProcessing() bool

	/* Distributed transactions */

	/* route hint always tx-block-level */
	SetCommitStrategy(value string)
	CommitStrategy() string

	/* Helpers for query binding */

	BindParams() [][]byte
	SetBindParams([][]byte)

	BindParamFormatCodes() []int16
	SetParamFormatCodes([]int16)

	/* These are non-guc */
	SetNextGID(string)
	NextGID() string

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

//revive:disable:var-naming
const (
	SPQR_DISTRIBUTION            = "__spqr__distribution"
	SPQR_DISTRIBUTED_RELATION    = "__spqr__distributed_relation"
	SPQR_DEFAULT_ROUTE_BEHAVIOUR = "__spqr__default_route_behaviour"
	SPQR_AUTO_DISTRIBUTION       = "__spqr__auto_distribution"
	SPQR_DISTRIBUTION_KEY        = "__spqr__distribution_key"
	SPQR_SHARDING_KEY            = "__spqr__sharding_key"
	SPQR_PREFERRED_ENGINE        = "__spqr__preferred_engine"
	SPQR_COMMIT_STRATEGY         = "__spqr__commit_strategy"
	SPQR_TARGET_SESSION_ATTRS    = "__spqr__target_session_attrs"
	SPQR_EXECUTE_ON              = "__spqr__execute_on"
	SPQR_EXECUTE_HOST_FILTER     = "__spqr__execute_host_filter"

	/* backward compatibility */
	SPQR_TARGET_SESSION_ATTRS_ALIAS   = "target_session_attrs"
	SPQR_TARGET_SESSION_ATTRS_ALIAS_2 = "target-session-attrs"

	/* Boolean */
	SPQR_SCATTER_QUERY   = "__spqr__scatter_query"
	SPQR_REPLY_NOTICE    = "__spqr__reply_notice"
	SPQR_MAINTAIN_PARAMS = "__spqr__maintain_params"
	SPQR_ENGINE_V2       = "__spqr__engine_v2"

	/* XXX: should we ever disallow? */
	SPQR_ALLOW_SPLIT_UPDATE   = "__spqr__allow_split_update"
	SPQR_ALLOW_POSTPROCESSING = "__spqr__allow_postprocessing"

	SPQR_LINEARIZE_DISPATCH      = "__spqr__linearize_dispatch"
	SPQR_ALLOW_AUTOPROTECT_2PC   = "__spqr__allow_autoprotect_2pc"
	SPQR_ALLOW_FLUX_ACCESS       = "__spqr__flux_access"
	SPQR_SESSION_CONNECTIONS_PIN = "__spqr__session_connections_pin"
	SPQR_EAGER_CLEANUP_2PC       = "__spqr__eager_cleanup_2pc"

	SPQR_ADVISORY_LOCK_BEHAVIOUR = "__spqr__advisory_lock_behaviour"

	SPQR_NOTICE_MESSAGE_FORMAT = "__spqr__notice_message_format"

	/* Special case for default_transaction_read_only */

	PG_DEFAULT_TRANSACTION_READ_ONLY = "default_transaction_read_only"
)

//revive:enable:var-naming

func ApplyAutoConfGUC(name, val string) error {
	if ParamIsBoolean(name) {
		if guc, err := FindBoolGUC(name); err == nil {

			v, err := ParseBoolGUCValue(val)

			if err != nil {
				return err
			}
			guc.SetBoolBootValue(v)
		}
	} else if ParamIsString(name) {
		guc, err := FindStrGUC(name)
		if err != nil {
			return err
		}

		guc.SetStrBootValue(val)
	}
	return nil
}

func ParamIsBoolean(n string) bool {
	switch n {
	case SPQR_ALLOW_SPLIT_UPDATE,
		SPQR_ALLOW_POSTPROCESSING, SPQR_LINEARIZE_DISPATCH,
		SPQR_ALLOW_FLUX_ACCESS, SPQR_ALLOW_AUTOPROTECT_2PC, SPQR_SESSION_CONNECTIONS_PIN,
		SPQR_REPLY_NOTICE, SPQR_MAINTAIN_PARAMS, SPQR_EAGER_CLEANUP_2PC,
		SPQR_SCATTER_QUERY:
		return true
	default:
		return false
	}
}

func ParamIsString(n string) bool {
	switch n {
	case SPQR_ADVISORY_LOCK_BEHAVIOUR,
		SPQR_DEFAULT_ROUTE_BEHAVIOUR,
		SPQR_PREFERRED_ENGINE,
		SPQR_EXECUTE_ON,
		SPQR_EXECUTE_HOST_FILTER,
		SPQR_SHARDING_KEY,
		SPQR_DISTRIBUTION_KEY,
		SPQR_NOTICE_MESSAGE_FORMAT:
		return true
	default:
		return false
	}
}

func ParseBoolGUCValue(val string) (bool, error) {
	switch strings.ToLower(val) {
	case "true", "ok", "on":
		return true, nil
	case "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("malformed value for GUC: %v", val)
	}
}
