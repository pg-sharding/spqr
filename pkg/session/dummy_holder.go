package session

import (
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

type DummySessionParamHandler struct {
	b            [][]byte
	f            []int16
	distribution string
	behaviour    string
	key          string

	engineV2     bool
	preferEngine string
	eo           string
}

// PreferredEngine implements SessionParamsHolder.
func (t *DummySessionParamHandler) PreferredEngine() string {
	return t.preferEngine
}

// SetPreferredEngine implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetPreferredEngine(level string, val string) {
	t.preferEngine = val
}

// DistributedRelation implements SessionParamsHolder.
func (t *DummySessionParamHandler) DistributedRelation() string {
	return "dummy"
}

// SetDistributedRelation implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetDistributedRelation(string, string) {

}

// DB implements SessionParamsHolder.
func (t *DummySessionParamHandler) DB() string {
	return "dummy"
}

// Usr implements SessionParamsHolder.
func (t *DummySessionParamHandler) Usr() string {
	return "dummy"
}

// GetTsa implements SessionParamsHolder.
func (t *DummySessionParamHandler) GetTsa() tsa.TSA {
	return config.TargetSessionAttrsRW
}

// SetTsa implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetTsa(string, string) {

}

// EnhancedMultiShardProcessing implements SessionParamsHolder.
func (t *DummySessionParamHandler) EnhancedMultiShardProcessing() bool {
	return t.engineV2
}

// SetEnhancedMultiShardProcessing implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetEnhancedMultiShardProcessing(l string, v bool) {
	t.engineV2 = v
}

func (t *DummySessionParamHandler) CommitStrategy() string {
	return ""
}

func (t *DummySessionParamHandler) SetCommitStrategy(v string) {
}

// ExecuteOn implements SessionParamsHolder.
func (t *DummySessionParamHandler) ExecuteOn() string {
	return t.eo
}

// SetExecuteOn implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetExecuteOn(l string, v string) {
	t.eo = v
}

// DistributionKey implements SessionParamsHolder.
func (t *DummySessionParamHandler) DistributionKey() string {
	return ""
}

// SetDistributionKey implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetDistributionKey(string) {

}

// AutoDistribution implements SessionParamsHolder.
func (t *DummySessionParamHandler) AutoDistribution() string {
	return ""
}

// SetAutoDistribution implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetAutoDistribution(string) {

}

// MaintainParams implements SessionParamsHolder.
func (t *DummySessionParamHandler) MaintainParams() bool {
	return false
}

// SetMaintainParams implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetMaintainParams(string, bool) {
}

// SetShowNoticeMsg implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetShowNoticeMsg(string, bool) {
}

// ShowNoticeMsg implements SessionParamsHolder.
func (t *DummySessionParamHandler) ShowNoticeMsg() bool {
	return false
}

// BindParamFormatCodes implements SessionParamsHolder.
// BindParamFormatCodes returns the format codes for binding parameters.
func (t *DummySessionParamHandler) BindParamFormatCodes() []int16 {
	return t.f
}

// SetParamFormatCodes implements SessionParamsHolder.
// SetParamFormatCodes sets the format codes for the DummySessionParamHandler.
// It takes a slice of int16 values as the input and assigns it to the 'f' field of the DummySessionParamHandler.
func (t *DummySessionParamHandler) SetParamFormatCodes(f []int16) {
	t.f = f
}

// NewDummyHandler creates a new instance of DummySessionParamHandler with the specified distribution.
// It returns a SessionParamsHolder interface.
func NewDummyHandler(distribution string) SessionParamsHolder {
	return &DummySessionParamHandler{
		distribution: distribution,
		engineV2:     false,
	}
}

// BindParams implements session.SessionParamsHolder.
// BindParams returns the byte slices stored in the DummySessionParamHandler.
func (t *DummySessionParamHandler) BindParams() [][]byte {
	return t.b
}

// Distribution implements session.SessionParamsHolder.
// Distribution returns the distribution value of the DummySessionParamHandler.
func (t *DummySessionParamHandler) Distribution() string {
	return t.distribution
}

// DefaultRouteBehaviour implements session.SessionParamsHolder.
// DefaultRouteBehaviour returns the default behavior for the route.
func (t *DummySessionParamHandler) DefaultRouteBehaviour() string {
	return t.behaviour
}

// ScatterQuery implements session.SessionParamsHolder.
// ScatterQuery returns the route hint associated with the DummySessionParamHandler.
func (t *DummySessionParamHandler) ScatterQuery() bool {
	return false
}

// SetBindParams implements session.SessionParamsHolder.
// SetBindParams sets the bind parameters for the DummySessionParamHandler.
// It takes a 2D byte slice as input and assigns it to the 'b' field of the struct.
func (t *DummySessionParamHandler) SetBindParams(b [][]byte) {
	t.b = b
}

// SetDistribution implements session.SessionParamsHolder.
// SetDistribution sets the distribution for the DummySessionParamHandler.
func (t *DummySessionParamHandler) SetDistribution(l string, d string) {
	t.distribution = d
}

// DistributionIsDefault implements session.SessionParamsHolder.
// DistributionIsDefault returns whether the distribution is set to default.
func (t *DummySessionParamHandler) DistributionIsDefault() bool {
	return false
}

// SetDefaultRouteBehaviour implements session.SessionParamsHolder.
// SetDefaultRouteBehaviour sets the default route behaviour for the DummySessionParamHandler.
// It takes a string parameter `b` representing the behaviour and assigns it to the `behaviour` field of the DummySessionParamHandler struct.
func (t *DummySessionParamHandler) SetDefaultRouteBehaviour(l string, b string) {
	t.behaviour = b
}

// SetScatterQuery implements session.SessionParamsHolder.
// SetScatterQuery sets the route hint for the DummySessionParamHandler.
// It takes a route hint as a parameter and assigns it to the `rh` field of the DummySessionParamHandler.
func (t *DummySessionParamHandler) SetScatterQuery(bool) {
}

// SetShardingKey implements session.SessionParamsHolder.
// SetShardingKey sets the sharding key for the DummySessionParamHandler.
// The sharding key is used to determine the partition or shard where the data will be stored.
func (t *DummySessionParamHandler) SetShardingKey(l string, k string) {
	t.key = k
}

// ShardingKey implements session.SessionParamsHolder.
// ShardingKey returns the sharding key associated with the DummySessionParamHandler.
func (t *DummySessionParamHandler) ShardingKey() string {
	return t.key
}

var _ SessionParamsHolder = &DummySessionParamHandler{}
