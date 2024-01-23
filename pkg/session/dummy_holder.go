package session

import "github.com/pg-sharding/spqr/router/routehint"

type DummySessionParamHandler struct {
	b         [][]byte
	dataspace string
	behaviour string
	key       string
	rh        routehint.RouteHint
}

func NewDummyHandler(dataspace string) SessionParamsHolder {
	return &DummySessionParamHandler{
		dataspace: dataspace,
		rh:        routehint.EmptyRouteHint{},
	}
}

// BindParams implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) BindParams() [][]byte {
	return t.b
}

// Keyspace implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) Keyspace() string {
	return t.dataspace
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

// SetKeyspace implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) SetKeyspace(d string) {
	t.dataspace = d
}

func (t *DummySessionParamHandler) KeyspaceIsDefault() bool {
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
