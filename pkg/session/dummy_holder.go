package session

import "github.com/pg-sharding/spqr/router/routehint"

type DummySessionParamHandler struct {
	b            [][]byte
	f            []int16
	distribution string
	behaviour    string
	key          string
	rh           routehint.RouteHint

	ms bool
	eo string
}

// ExecuteOn implements SessionParamsHolder.
func (t *DummySessionParamHandler) ExecuteOn() string {
	return t.eo
}

// SetExecuteOn implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetExecuteOn(l bool, v string) {
	t.eo = v
}

// AllowMultishard implements SessionParamsHolder.
func (t *DummySessionParamHandler) AllowMultishard() bool {
	return t.ms
}

// SetAllowMultishard implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetAllowMultishard(l bool, v bool) {
	t.ms = v
}

// DistributionKey implements SessionParamsHolder.
func (t *DummySessionParamHandler) DistributionKey() string {
	return ""
}

// SetDistributionKey implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetDistributionKey(bool, string) {

}

// AutoDistribution implements SessionParamsHolder.
func (t *DummySessionParamHandler) AutoDistribution() string {
	return ""
}

// SetAutoDistribution implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetAutoDistribution(bool, string) {

}

// MaintainParams implements SessionParamsHolder.
func (t *DummySessionParamHandler) MaintainParams() bool {
	return false
}

// SetMaintainParams implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetMaintainParams(bool) {
}

// SetShowNoticeMsg implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetShowNoticeMsg(bool) {
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
		rh:           routehint.EmptyRouteHint{},
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
func (t *DummySessionParamHandler) SetDistribution(l bool, d string) {
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
func (t *DummySessionParamHandler) SetDefaultRouteBehaviour(l bool, b string) {
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
func (t *DummySessionParamHandler) SetShardingKey(l bool, k string) {
	t.key = k
}

// ShardingKey implements session.SessionParamsHolder.
// ShardingKey returns the sharding key associated with the DummySessionParamHandler.
func (t *DummySessionParamHandler) ShardingKey() string {
	return t.key
}

var _ SessionParamsHolder = &DummySessionParamHandler{}
