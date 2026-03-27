package session

import (
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

type ParamHistory interface {
	Commit()
	Set(entry ParamEntry)
	RollbackTo(txCnt int)

	CleanupStatementSet()
}

type ParamEntry struct {
	Tx    int
	Value string

	// simple
	IsLocal bool

	// virtual
	Levels map[string]string
}

func (lhs ParamEntry) EqualIgnoringValue(rhs ParamEntry) bool {
	return lhs.Tx == rhs.Tx && lhs.IsLocal == rhs.IsLocal
}

type SimpleSessionParamHandler struct {
	params map[string]ParamHistory

	activeParamSet map[string]string
	virtualParams  map[string]struct{}

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
	maintain_params    bool
}

func (cl *SimpleSessionParamHandler) resolveVirtualBoolParam(name string, defaultVal bool) bool {
	v, ok := cl.activeParamSet[name]
	if ok {
		return v == "ok"
	}
	return defaultVal
}

func (cl *SimpleSessionParamHandler) recordVirtualParam(level string, name string, val string) {
	cl.getParamHistory(name, true).Set(ParamEntry{
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
	cl.recordVirtualParam(level, SPQR_DISTRIBUTION, val)
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
	cl.recordVirtualParam(level, SPQR_PREFERRED_ENGINE, val)
}

// AllowSplitUpdate implements client.Client.
func (cl *SimpleSessionParamHandler) AllowSplitUpdate() bool {
	return cl.resolveVirtualBoolParam(SPQR_ALLOW_SPLIT_UPDATE, config.RouterConfig().Qr.AllowSplitUpdate)
}

// SetAllowSplitUpdate implements client.Client.
func (cl *SimpleSessionParamHandler) SetAllowSplitUpdate(level string, val bool) {
	if val {
		cl.recordVirtualParam(level, SPQR_ALLOW_SPLIT_UPDATE, "ok")
	} else {
		cl.recordVirtualParam(level, SPQR_ALLOW_SPLIT_UPDATE, "no")
	}
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

/* TX management */

func (cl *SimpleSessionParamHandler) CommitActiveSet() {
	cl.savepointTxCounter = map[string]int{}

	for _, history := range cl.params {
		history.Commit()
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
			cl.getParamHistory(opname, false).Set(ParamEntry{
				Tx:      cl.txCnt,
				Value:   opvalue,
				IsLocal: isLocal,
			})
		}

	} else {
		cl.getParamHistory(name, false).Set(ParamEntry{
			Tx:      cl.txCnt,
			Value:   value,
			IsLocal: isLocal,
		})
	}
}

func (cl *SimpleSessionParamHandler) ResetAll() {
	cl.activeParamSet = cl.startupParameters
	cl.params = map[string]ParamHistory{}
}

func (cl *SimpleSessionParamHandler) RollbackToSP(name string) {
	targetTxCnt := cl.savepointTxCounter[name]

	for _, history := range cl.params {
		history.RollbackTo(targetTxCnt)
	}

	cl.txCnt = targetTxCnt + 1
}

func (cl *SimpleSessionParamHandler) ResetParam(name string) {
	delete(cl.params, name)

	if val, ok := cl.startupParameters[name]; ok {
		cl.activeParamSet[name] = val
	} else {
		delete(cl.activeParamSet, name)
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
	for _, history := range cl.params {
		history.CleanupStatementSet()
	}
}

func (cl *SimpleSessionParamHandler) Savepoint(name string) {
	cl.savepointTxCounter[name] = cl.txCnt
	cl.txCnt++
}

func (cl *SimpleSessionParamHandler) Rollback() {
	cl.savepointTxCounter = map[string]int{}

	for _, history := range cl.params {
		history.RollbackTo(0)
	}

	cl.txCnt = 0
}

func (cl *SimpleSessionParamHandler) SetStartupParams(m map[string]string) {
	cl.startupParameters = m
}

func (cl *SimpleSessionParamHandler) getParamHistory(name string, isVirtual bool) ParamHistory {
	if h, ok := cl.params[name]; ok {
		return h
	} else {
		var h ParamHistory
		if isVirtual {
			h = &VirtualParamHistory{globalMap: cl.activeParamSet, name: name}
		} else {
			h = &SimpleParamHistory{globalMap: cl.activeParamSet, name: name}
		}
		cl.params[name] = h
		return h
	}
}

func NewSimpleHandler(t string, show_notice bool, ds string, defaultRouteBehaviour string) SessionParamsHolder {
	return &SimpleSessionParamHandler{
		params: map[string]ParamHistory{},

		startupParameters: map[string]string{},
		virtualParams:     map[string]struct{}{},

		activeParamSet: map[string]string{
			SPQR_DISTRIBUTION:            "default",
			SPQR_DEFAULT_ROUTE_BEHAVIOUR: defaultRouteBehaviour,
		},
		defaultTsa:            t,
		showNoticeMessages:    show_notice,
		defaultCommitStrategy: ds,
	}
}
