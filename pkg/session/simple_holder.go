package session

import (
	"fmt"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

type ParamVisibility interface {
	Commit()
	Set(entry ParamEntry)
	RollbackTo(txCnt int)
	Reset(txCnt int, defaultValue *string)

	CleanupStatementSet()
}

type ParamAction int

const (
	ParamActionSet = iota
	ParamActionReset
)

type ParamEntry struct {
	Tx    int
	Value string

	Action ParamAction

	// simple
	IsLocal bool

	// virtual
	Levels map[string]string
}

type BoolGUCimpl struct {
	n         string
	shortName string
	def       func() bool
}

func (guc BoolGUCimpl) Set(cl SessionParamsHolder, level string, val bool) {
	if val {
		cl.RecordVirtualParam(level, guc.n, "ok")
	} else {
		cl.RecordVirtualParam(level, guc.n, "no")
	}
}

func (guc BoolGUCimpl) ShortName() string {
	return guc.shortName
}

func (guc BoolGUCimpl) Reset() {

}

func (guc BoolGUCimpl) Get(cl SessionParamsHolder) bool {
	return cl.ResolveVirtualBoolParam(guc.n, guc.def())
}

func (lhs ParamEntry) EqualIgnoringValue(rhs ParamEntry) bool {
	return lhs.Tx == rhs.Tx && lhs.IsLocal == rhs.IsLocal
}

type SimpleSessionParamHandler struct {
	params map[string]ParamVisibility

	activeParamSet map[string]string

	startupParameters map[string]string

	savepointTxCounter map[string]int

	txCnt int

	/* target-session-attrs */
	defaultTsa            string
	defaultCommitStrategy string

	usr string

	bindParams [][]byte

	paramCodes []int16

	showNoticeMessages bool
	maintainParams    bool
}

func (cl *SimpleSessionParamHandler) ResolveVirtualBoolParam(name string, defaultVal bool) bool {
	v, ok := cl.activeParamSet[name]
	if ok {
		return v == "ok"
	}
	return defaultVal
}

func (cl *SimpleSessionParamHandler) RecordVirtualParam(level string, name string, val string) {
	cl.getParamVisibility(name, true).Set(ParamEntry{
		Tx: cl.txCnt,
		Levels: map[string]string{
			level: val,
		},
	})
}

func (cl *SimpleSessionParamHandler) resolveVirtualStringParam(name string, defaultVal string) string {
	v, ok := cl.activeParamSet[name]
	if ok {
		return v
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
	cl.RecordVirtualParam(level, SPQR_DISTRIBUTION, val)
}

// Distribution implements RouterClient.
func (cl *SimpleSessionParamHandler) Distribution() string {
	return cl.resolveVirtualStringParam(SPQR_DISTRIBUTION, "")
}

// PreferredEngine implements client.Client.
func (cl *SimpleSessionParamHandler) PreferredEngine() string {
	return cl.resolveVirtualStringParam(SPQR_PREFERRED_ENGINE, "")
}

// SetPreferredEngine implements client.Client.
func (cl *SimpleSessionParamHandler) SetPreferredEngine(level string, val string) {
	cl.RecordVirtualParam(level, SPQR_PREFERRED_ENGINE, val)
}

// SetDistributedRelation implements RouterClient.
func (cl *SimpleSessionParamHandler) SetDistributedRelation(level string, val string) {
	cl.RecordVirtualParam(level, SPQR_DISTRIBUTED_RELATION, val)
}

// DistributedRelation implements RouterClient.
func (cl *SimpleSessionParamHandler) DistributedRelation() string {
	return cl.resolveVirtualStringParam(SPQR_DISTRIBUTED_RELATION, "")
}

// SetExecuteOn implements RouterClient.
func (cl *SimpleSessionParamHandler) SetExecuteOn(level string, val string) {
	cl.RecordVirtualParam(level, SPQR_EXECUTE_ON, val)
}

// ExecuteOn implements RouterClient.
func (cl *SimpleSessionParamHandler) ExecuteOn() string {
	return cl.resolveVirtualStringParam(SPQR_EXECUTE_ON, "")
}

// SetExecuteOn implements RouterClient.
func (cl *SimpleSessionParamHandler) SetEnhancedMultiShardProcessing(level string, val bool) {
	if val {
		cl.RecordVirtualParam(level, SPQR_ENGINE_V2, "ok")
	} else {
		cl.RecordVirtualParam(level, SPQR_ENGINE_V2, "no")
	}
}

// ExecuteOn implements RouterClient.
func (cl *SimpleSessionParamHandler) EnhancedMultiShardProcessing() bool {
	return cl.ResolveVirtualBoolParam(SPQR_ENGINE_V2, config.RouterConfig().Qr.EnhancedMultiShardProcessing)
}

func (cl *SimpleSessionParamHandler) SetCommitStrategy(val string) {
	cl.RecordVirtualParam(VirtualParamLevelTxBlock, SPQR_COMMIT_STRATEGY, val)
}

func (cl *SimpleSessionParamHandler) CommitStrategy() string {
	return cl.resolveVirtualStringParam(SPQR_COMMIT_STRATEGY, cl.defaultCommitStrategy)
}

// SetAutoDistribution implements RouterClient.
func (cl *SimpleSessionParamHandler) SetAutoDistribution(val string) {
	cl.RecordVirtualParam(VirtualParamLevelStatement, SPQR_AUTO_DISTRIBUTION, val)
}

// AutoDistribution implements RouterClient.
func (cl *SimpleSessionParamHandler) AutoDistribution() string {
	return cl.resolveVirtualStringParam(SPQR_AUTO_DISTRIBUTION, "")
}

// SetDistributionKey implements RouterClient.
func (cl *SimpleSessionParamHandler) SetDistributionKey(val string) {
	cl.RecordVirtualParam(VirtualParamLevelStatement, SPQR_DISTRIBUTION_KEY, val)
}

// DistributionKey implements RouterClient.
func (cl *SimpleSessionParamHandler) DistributionKey() string {
	return cl.resolveVirtualStringParam(SPQR_DISTRIBUTION_KEY, "")
}

// MaintainParams implements RouterClient.
func (cl *SimpleSessionParamHandler) MaintainParams() bool {
	return cl.maintainParams
}

// SetMaintainParams implements RouterClient.
func (cl *SimpleSessionParamHandler) SetMaintainParams(_ string, val bool) {
	cl.maintainParams = val
}

// SetShowNoticeMsg implements client.Client.
func (cl *SimpleSessionParamHandler) SetShowNoticeMsg(_ string, val bool) {
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
	cl.RecordVirtualParam(level, SPQR_SHARDING_KEY, k)
}

// ShardingKey implements RouterClient.
func (cl *SimpleSessionParamHandler) ShardingKey() string {
	return cl.resolveVirtualStringParam(SPQR_SHARDING_KEY, "")
}

// SetDefaultRouteBehaviour implements RouterClient.
func (cl *SimpleSessionParamHandler) SetDefaultRouteBehaviour(level string, b string) {
	cl.RecordVirtualParam(level, SPQR_DEFAULT_ROUTE_BEHAVIOUR, b)
}

// DefaultRouteBehaviour implements RouterClient.
func (cl *SimpleSessionParamHandler) DefaultRouteBehaviour() string {
	return cl.resolveVirtualStringParam(SPQR_DEFAULT_ROUTE_BEHAVIOUR, "")
}

// ScatterQuery implements RouterClient.
func (cl *SimpleSessionParamHandler) ScatterQuery() bool {
	return cl.ResolveVirtualBoolParam(SPQR_SCATTER_QUERY, false)
}

// SetScatterQuery implements RouterClient.
func (cl *SimpleSessionParamHandler) SetScatterQuery(val bool) {
	if val {
		cl.RecordVirtualParam(VirtualParamLevelStatement, SPQR_SCATTER_QUERY, "ok")
	} else {
		cl.RecordVirtualParam(VirtualParamLevelStatement, SPQR_SCATTER_QUERY, "no")
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
		cl.RecordVirtualParam(level, SPQR_TARGET_SESSION_ATTRS, s)
	default:
		// XXX: else error out!
	}
}

func (cl *SimpleSessionParamHandler) ResetTsa() {
	cl.SetTsa(VirtualParamLevelTxBlock, cl.defaultTsa)
}

/* TX management */

func (cl *SimpleSessionParamHandler) CommitActiveSet() {
	cl.savepointTxCounter = map[string]int{}

	for _, vis := range cl.params {
		vis.Commit()
	}

	cl.txCnt = 0
}

func (cl *SimpleSessionParamHandler) Params() map[string]string {
	return cl.activeParamSet
}

func (cl *SimpleSessionParamHandler) SetParam(name, value string, isLocal bool) {
	spqrlog.Zero.Debug().
		Str("name", name).
		Str("value", value).
		Msg("client param")
	if name == "options" {
		i := 0
		j := 0
		for i < len(value) {
			if value[i] == ' ' {
				i++
				continue
			}
			if value[i] == '-' {
				if i+2 == len(value) || value[i+1] != 'c' {
					// bad
					return
				}
			}
			i += 3
			j = i

			opname := ""
			opvalue := ""

			for j < len(value) {
				if value[j] == '=' {
					j++
					break
				}
				opname += string(value[j])
				j++
			}

			for j < len(value) {
				if value[j] == ' ' {
					break
				}
				opvalue += string(value[j])
				j++
			}

			if len(opname) == 0 || len(opvalue) == 0 {
				// bad
				return
			}
			i = j + 1

			spqrlog.Zero.Debug().
				Str("opname", opname).
				Str("opvalue", opvalue).
				Msg("parsed pgoption param")
			cl.getParamVisibility(opname, false).Set(ParamEntry{
				Tx:      cl.txCnt,
				Value:   opvalue,
				IsLocal: isLocal,
				Levels:  map[string]string{},
			})
		}

	} else {
		cl.getParamVisibility(name, false).Set(ParamEntry{
			Tx:      cl.txCnt,
			Value:   value,
			IsLocal: isLocal,
			Levels:  map[string]string{},
		})
	}
}

func (cl *SimpleSessionParamHandler) ResetAll() {
	for param := range cl.params {
		cl.ResetParam(param)
	}
	for k := range cl.startupParameters {
		cl.ResetParam(k)
	}
}

func (cl *SimpleSessionParamHandler) RollbackToSP(name string) {
	targetTxCnt := cl.savepointTxCounter[name]

	for _, vis := range cl.params {
		vis.RollbackTo(targetTxCnt)
	}

	cl.txCnt = targetTxCnt + 1
}

func (cl *SimpleSessionParamHandler) ResetParam(name string) {
	if vis, ok := cl.params[name]; ok {
		var defaultValue *string
		if v, ok := cl.startupParameters[name]; ok {
			defaultValue = &v
		}
		vis.Reset(cl.txCnt, defaultValue)
	}

	spqrlog.Zero.Debug().
		Interface("activeParamSet", cl.activeParamSet).
		Msg("activeParamSet are now")
}

func (cl *SimpleSessionParamHandler) StartTx() {
	cl.savepointTxCounter = map[string]int{}
	cl.txCnt = 1
}

func (cl *SimpleSessionParamHandler) CleanupStatementSet() {
	for _, vis := range cl.params {
		vis.CleanupStatementSet()
	}
}

func (cl *SimpleSessionParamHandler) Savepoint(name string) {
	cl.savepointTxCounter[name] = cl.txCnt
	cl.txCnt++
}

func (cl *SimpleSessionParamHandler) Rollback() {
	cl.savepointTxCounter = map[string]int{}

	for _, vis := range cl.params {
		vis.RollbackTo(0)
	}

	cl.txCnt = 0
}

func (cl *SimpleSessionParamHandler) SetStartupParams(m map[string]string) {
	cl.startupParameters = m
}

func (cl *SimpleSessionParamHandler) getParamVisibility(name string, isVirtual bool) ParamVisibility {
	if h, ok := cl.params[name]; ok {
		return h
	} else {
		var h ParamVisibility
		if isVirtual {
			h = &VirtualParamVisibility{globalMap: cl.activeParamSet, name: name}
		} else {
			h = &SimpleParamVisibility{globalMap: cl.activeParamSet, name: name}
		}
		cl.params[name] = h
		return h
	}
}

var boolGUCs []BoolGUCimpl = []BoolGUCimpl{
	{
		n:         SPQR_ALLOW_SPLIT_UPDATE,
		shortName: "allow split update",
		def: func() bool {
			return config.RouterConfig().Qr.AllowSplitUpdate
		},
	},
	{
		n:         SPQR_ALLOW_POSTPROCESSING,
		shortName: "allow postprocessing",
		def: func() bool {
			return config.RouterConfig().Qr.AllowPostProcessing
		},
	},
	{
		n:         SPQR_LINEARIZE_DISPATCH,
		shortName: "linearize dispatch",
		def: func() bool {
			return false
		},
	},
}

func (cl *SimpleSessionParamHandler) FindBoolGUC(n string) (BoolGUC, error) {
	for _, guc := range boolGUCs {
		if guc.n == n {
			return guc, nil
		}
	}

	return nil, fmt.Errorf("unknown GUC: %s", n)
}

func NewSimpleHandler(t string, showNotice bool, ds string, defaultRouteBehaviour string) SessionParamsHolder {
	return &SimpleSessionParamHandler{
		params: map[string]ParamVisibility{},

		startupParameters: map[string]string{},

		activeParamSet: map[string]string{
			SPQR_DISTRIBUTION:            "default",
			SPQR_DEFAULT_ROUTE_BEHAVIOUR: defaultRouteBehaviour,
		},
		defaultTsa:            t,
		showNoticeMessages:    showNotice,
		defaultCommitStrategy: ds,
	}
}
