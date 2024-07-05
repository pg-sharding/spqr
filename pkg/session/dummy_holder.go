package session

import "github.com/pg-sharding/spqr/router/routehint"

type DummySessionParamHandler struct {
	b            [][]byte
	f            []int16
	distribution string
	behaviour    string
	key          string
	rh           routehint.RouteHint
}

// MaintainParams implements SessionParamsHolder.
func (t *DummySessionParamHandler) MaintainParams() bool {
	panic("unimplemented")
}

// SetMaintainParams implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetMaintainParams(bool) {
	panic("unimplemented")
}

// SetShowNoticeMsg implements SessionParamsHolder.
func (t *DummySessionParamHandler) SetShowNoticeMsg(bool) {
	panic("unimplemented")
}

// ShowNoticeMsg implements SessionParamsHolder.
func (t *DummySessionParamHandler) ShowNoticeMsg() bool {
	panic("unimplemented")
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

// RouteHint implements session.SessionParamsHolder.
// RouteHint returns the route hint associated with the DummySessionParamHandler.
func (t *DummySessionParamHandler) RouteHint() routehint.RouteHint {
	return t.rh
}

// SetBindParams implements session.SessionParamsHolder.
// SetBindParams sets the bind parameters for the DummySessionParamHandler.
// It takes a 2D byte slice as input and assigns it to the 'b' field of the struct.
func (t *DummySessionParamHandler) SetBindParams(b [][]byte) {
	t.b = b
}

// SetDistribution implements session.SessionParamsHolder.
// SetDistribution sets the distribution for the DummySessionParamHandler.
func (t *DummySessionParamHandler) SetDistribution(d string) {
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
func (t *DummySessionParamHandler) SetDefaultRouteBehaviour(b string) {
	t.behaviour = b
}

// SetRouteHint implements session.SessionParamsHolder.
// SetRouteHint sets the route hint for the DummySessionParamHandler.
// It takes a route hint as a parameter and assigns it to the `rh` field of the DummySessionParamHandler.
func (t *DummySessionParamHandler) SetRouteHint(rh routehint.RouteHint) {
	t.rh = rh
}

// SetShardingKey implements session.SessionParamsHolder.
// SetShardingKey sets the sharding key for the DummySessionParamHandler.
// The sharding key is used to determine the partition or shard where the data will be stored.
func (t *DummySessionParamHandler) SetShardingKey(k string) {
	t.key = k
}

// ShardingKey implements session.SessionParamsHolder.
// ShardingKey returns the sharding key associated with the DummySessionParamHandler.
func (t *DummySessionParamHandler) ShardingKey() string {
	return t.key
}

var _ SessionParamsHolder = &DummySessionParamHandler{}
