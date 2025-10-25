package session

import (
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

type SimpleSessionParamHandler struct {
	beginTxParamSet   map[string]string
	localTxParamSet   map[string]string
	statementParamSet map[string]string
	activeParamSet    map[string]string

	/* target-session-attrs */
	defaultTsa            string
	defaultCommitStrategy string

	usr string

	bindParams [][]byte

	paramCodes []int16

	showNoticeMessages bool
	maintain_params    bool
}

func (cl *SimpleSessionParamHandler) resolveVirtualBoolParam(name string, defaultVal bool) bool {
	if val, ok := cl.localTxParamSet[name]; ok {
		return val == "ok"
	}
	if val, ok := cl.statementParamSet[name]; ok {
		return val == "ok"
	}
	if val, ok := cl.activeParamSet[name]; ok {
		return val == "ok"
	}
	return defaultVal
}

func (cl *SimpleSessionParamHandler) recordVirtualParam(level string, name string, val string) {
	switch level {
	case VirtualParamLevelLocal:
		cl.localTxParamSet[name] = val
	case VirtualParamLevelStatement:
		cl.statementParamSet[name] = val
	default:
		cl.activeParamSet[name] = val
	}
}

func (cl *SimpleSessionParamHandler) resolveVirtualStringParam(name string, defaultVal string) string {
	if val, ok := cl.localTxParamSet[name]; ok {
		return val
	}
	if val, ok := cl.statementParamSet[name]; ok {
		return val
	}
	if val, ok := cl.activeParamSet[name]; ok {
		return val
	}
	return defaultVal
}

// Usr implements SessionParamsHolder.
func (cl *SimpleSessionParamHandler) Usr() string {
	return cl.usr
}

// SetUsr implements SessionParamsHolder.
func (cl *SimpleSessionParamHandler) SetUsr(u string) {
	cl.usr = u
}

// SetDistribution implements RouterClient.
func (cl *SimpleSessionParamHandler) SetDistribution(level string, val string) {
	cl.recordVirtualParam(level, SPQR_DISTRIBUTION, val)
}

// Distribution implements RouterClient.
func (cl *SimpleSessionParamHandler) Distribution() string {
	return cl.resolveVirtualStringParam(SPQR_DISTRIBUTION, "")
}

// PreferredEngine implements client.Client.
func (cl *SimpleSessionParamHandler) PreferredEngine() string {
	return cl.resolveVirtualStringParam(SPQR_DISTRIBUTION, "")
}

// SetPreferredEngine implements client.Client.
func (cl *SimpleSessionParamHandler) SetPreferredEngine(level string, val string) {

	cl.recordVirtualParam(level, SPQR_DISTRIBUTION, val)
}

// SetDistributedRelation implements RouterClient.
func (cl *SimpleSessionParamHandler) SetDistributedRelation(level string, val string) {
	cl.recordVirtualParam(level, SPQR_DISTRIBUTED_RELATION, val)
}

// DistributedRelation implements RouterClient.
func (cl *SimpleSessionParamHandler) DistributedRelation() string {
	return cl.resolveVirtualStringParam(SPQR_DISTRIBUTED_RELATION, "")
}

// SetExecuteOn implements RouterClient.
func (cl *SimpleSessionParamHandler) SetExecuteOn(level string, val string) {
	cl.recordVirtualParam(level, SPQR_EXECUTE_ON, val)
}

// ExecuteOn implements RouterClient.
func (cl *SimpleSessionParamHandler) ExecuteOn() string {
	return cl.resolveVirtualStringParam(SPQR_EXECUTE_ON, "")
}

// SetExecuteOn implements RouterClient.
func (cl *SimpleSessionParamHandler) SetEnhancedMultiShardProcessing(level string, val bool) {
	if val {
		cl.recordVirtualParam(level, SPQR_ENGINE_V2, "ok")
	} else {
		cl.recordVirtualParam(level, SPQR_ENGINE_V2, "no")
	}
}

// ExecuteOn implements RouterClient.
func (cl *SimpleSessionParamHandler) EnhancedMultiShardProcessing() bool {
	return cl.resolveVirtualBoolParam(SPQR_ENGINE_V2, config.RouterConfig().Qr.EnhancedMultiShardProcessing)
}

func (cl *SimpleSessionParamHandler) SetCommitStrategy(val string) {
	cl.recordVirtualParam(VirtualParamLevelTxBlock, SPQR_COMMIT_STRATEGY, val)
}

func (cl *SimpleSessionParamHandler) CommitStrategy() string {
	return cl.resolveVirtualStringParam(SPQR_COMMIT_STRATEGY, cl.defaultCommitStrategy)
}

// SetAutoDistribution implements RouterClient.
func (cl *SimpleSessionParamHandler) SetAutoDistribution(val string) {
	cl.recordVirtualParam(VirtualParamLevelStatement, SPQR_AUTO_DISTRIBUTION, val)
}

// AutoDistribution implements RouterClient.
func (cl *SimpleSessionParamHandler) AutoDistribution() string {
	return cl.resolveVirtualStringParam(SPQR_AUTO_DISTRIBUTION, "")
}

// SetDistributionKey implements RouterClient.
func (cl *SimpleSessionParamHandler) SetDistributionKey(val string) {
	cl.recordVirtualParam(VirtualParamLevelStatement, SPQR_DISTRIBUTION_KEY, val)
}

// DistributionKey implements RouterClient.
func (cl *SimpleSessionParamHandler) DistributionKey() string {
	return cl.resolveVirtualStringParam(SPQR_DISTRIBUTION_KEY, "")
}

// MaintainParams implements RouterClient.
func (cl *SimpleSessionParamHandler) MaintainParams() bool {
	return cl.maintain_params
}

// SetMaintainParams implements RouterClient.
func (cl *SimpleSessionParamHandler) SetMaintainParams(level string, val bool) {
	cl.maintain_params = val
}

// SetShowNoticeMsg implements client.Client.
func (cl *SimpleSessionParamHandler) SetShowNoticeMsg(level string, val bool) {
	cl.showNoticeMessages = val
}

// ShowNoticeMsg implements RouterClient.
func (cl *SimpleSessionParamHandler) ShowNoticeMsg() bool {
	return cl.showNoticeMessages
}

// BindParamFormatCodes implements RouterClient.
func (cl *SimpleSessionParamHandler) BindParamFormatCodes() []int16 {
	return cl.paramCodes
}

// SetParamFormatCodes implements RouterClient.
func (cl *SimpleSessionParamHandler) SetParamFormatCodes(paramCodes []int16) {
	cl.paramCodes = paramCodes
}

// BindParams implements RouterClient.
func (cl *SimpleSessionParamHandler) BindParams() [][]byte {
	return cl.bindParams
}

// SetBindParams implements RouterClient.
func (cl *SimpleSessionParamHandler) SetBindParams(p [][]byte) {
	cl.bindParams = p
}

// SetShardingKey implements RouterClient.
func (cl *SimpleSessionParamHandler) SetShardingKey(level string, k string) {
	cl.recordVirtualParam(level, SPQR_SHARDING_KEY, k)
}

// ShardingKey implements RouterClient.
func (cl *SimpleSessionParamHandler) ShardingKey() string {
	return cl.resolveVirtualStringParam(SPQR_SHARDING_KEY, "")
}

// SetDefaultRouteBehaviour implements RouterClient.
func (cl *SimpleSessionParamHandler) SetDefaultRouteBehaviour(level string, b string) {
	cl.recordVirtualParam(level, SPQR_DEFAULT_ROUTE_BEHAVIOUR, b)
}

// DefaultRouteBehaviour implements RouterClient.
func (cl *SimpleSessionParamHandler) DefaultRouteBehaviour() string {
	return cl.resolveVirtualStringParam(SPQR_DEFAULT_ROUTE_BEHAVIOUR, "")
}

// ScatterQuery implements RouterClient.
func (cl *SimpleSessionParamHandler) ScatterQuery() bool {
	return cl.resolveVirtualBoolParam(SPQR_SCATTER_QUERY, false)
}

// SetScatterQuery implements RouterClient.
func (cl *SimpleSessionParamHandler) SetScatterQuery(val bool) {
	if val {
		cl.recordVirtualParam(VirtualParamLevelStatement, SPQR_SCATTER_QUERY, "ok")
	} else {
		cl.recordVirtualParam(VirtualParamLevelStatement, SPQR_SCATTER_QUERY, "no")
	}
}

func (cl *SimpleSessionParamHandler) GetTsa() tsa.TSA {
	return tsa.TSA(cl.resolveVirtualStringParam(SPQR_TARGET_SESSION_ATTRS, cl.defaultTsa))
}

func (cl *SimpleSessionParamHandler) SetTsa(level string, s string) {
	switch s {
	case config.TargetSessionAttrsAny,
		config.TargetSessionAttrsPS,
		config.TargetSessionAttrsPR,
		config.TargetSessionAttrsRW,
		config.TargetSessionAttrsSmartRW,
		config.TargetSessionAttrsRO:
		cl.recordVirtualParam(level, SPQR_TARGET_SESSION_ATTRS, s)
	default:
		// XXX: else error out!
	}
}

func (cl *SimpleSessionParamHandler) ResetTsa() {
	cl.SetTsa(VirtualParamLevelTxBlock, cl.defaultTsa)
}

func NewSimpleHandler(t string, show_notice bool, ds string) SessionParamsHolder {
	return &SimpleSessionParamHandler{
		beginTxParamSet:       map[string]string{},
		localTxParamSet:       map[string]string{},
		statementParamSet:     map[string]string{},
		activeParamSet:        map[string]string{},
		defaultTsa:            t,
		showNoticeMessages:    show_notice,
		defaultCommitStrategy: ds,
	}
}
