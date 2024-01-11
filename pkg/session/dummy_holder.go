package session

import "github.com/pg-sharding/spqr/router/routehint"

type DummySessionParamHandler struct {
	b                [][]byte
	dataspace        string
	dataspaceChanged bool
	behaviour        string
	key              string
	rh               routehint.RouteHint
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

// Dataspace implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) Dataspace() string {
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

// SetDataspace implements session.SessionParamsHolder.
func (t *DummySessionParamHandler) SetDataspace(d string) {
	t.dataspace = d
	t.dataspaceChanged = true
}

func (t *DummySessionParamHandler) DataspaceIsDefault() bool {
	return !t.dataspaceChanged
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
