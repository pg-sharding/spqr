package session

import "github.com/pg-sharding/spqr/router/routehint"

type DummySessionParamHandler struct {
	b            [][]byte
	distrinution string
	behaviour    string
	key          string
	rh           routehint.RouteHint
}

func NewDummyHandler(distrinution string) SessionParamsHolder {
	return &DummySessionParamHandler{
		distrinution: distrinution,
		rh:           routehint.EmptyRouteHint{},
	}
}

// BindParams implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) BindParams() [][]byte {
	return t.b
}

// Distribution implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) Distribution() string {
	return t.distrinution
}

// DefaultRouteBehaviour implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) DefaultRouteBehaviour() string {
	return t.behaviour
}

// RouteHint implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) RouteHint() routehint.RouteHint {
	return t.rh
}

// SetBindParams implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) SetBindParams(b [][]byte) {
	t.b = b
}

// SetDistribution implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) SetDistribution(d string) {
	t.distrinution = d
}

func (t *DummySessionParamHandler) DistributionIsDefault() bool {
	return false
}

// SetDefaultRouteBehaviour implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) SetDefaultRouteBehaviour(b string) {
	t.behaviour = b
}

// SetRouteHint implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) SetRouteHint(rh routehint.RouteHint) {
	t.rh = rh
}

// SetShardingKey implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) SetShardingKey(k string) {
	t.key = k
}

// ShardingKey implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) ShardingKey() string {
	return t.key
}

var _ SessionParamsHolder = &DummySessionParamHandler{}
