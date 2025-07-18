// Code generated by MockGen. DO NOT EDIT.
// Source: ./pkg/pool/pool.go
//
// Generated by this command:
//
//	mockgen -source=./pkg/pool/pool.go -destination=pkg/mock/pool/mock_pool.go -package=mock
//

// Package mock is a generated GoMock package.
package mock

import (
	reflect "reflect"

	config "github.com/pg-sharding/spqr/pkg/config"
	kr "github.com/pg-sharding/spqr/pkg/models/kr"
	pool "github.com/pg-sharding/spqr/pkg/pool"
	shard "github.com/pg-sharding/spqr/pkg/shard"
	tsa "github.com/pg-sharding/spqr/pkg/tsa"
	gomock "go.uber.org/mock/gomock"
)

// MockConnectionKepper is a mock of ConnectionKepper interface.
type MockConnectionKepper struct {
	ctrl     *gomock.Controller
	recorder *MockConnectionKepperMockRecorder
	isgomock struct{}
}

// MockConnectionKepperMockRecorder is the mock recorder for MockConnectionKepper.
type MockConnectionKepperMockRecorder struct {
	mock *MockConnectionKepper
}

// NewMockConnectionKepper creates a new mock instance.
func NewMockConnectionKepper(ctrl *gomock.Controller) *MockConnectionKepper {
	mock := &MockConnectionKepper{ctrl: ctrl}
	mock.recorder = &MockConnectionKepperMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConnectionKepper) EXPECT() *MockConnectionKepperMockRecorder {
	return m.recorder
}

// Discard mocks base method.
func (m *MockConnectionKepper) Discard(sh shard.ShardHostInstance) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Discard", sh)
	ret0, _ := ret[0].(error)
	return ret0
}

// Discard indicates an expected call of Discard.
func (mr *MockConnectionKepperMockRecorder) Discard(sh any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Discard", reflect.TypeOf((*MockConnectionKepper)(nil).Discard), sh)
}

// Put mocks base method.
func (m *MockConnectionKepper) Put(host shard.ShardHostInstance) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Put", host)
	ret0, _ := ret[0].(error)
	return ret0
}

// Put indicates an expected call of Put.
func (mr *MockConnectionKepperMockRecorder) Put(host any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Put", reflect.TypeOf((*MockConnectionKepper)(nil).Put), host)
}

// View mocks base method.
func (m *MockConnectionKepper) View() pool.Statistics {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "View")
	ret0, _ := ret[0].(pool.Statistics)
	return ret0
}

// View indicates an expected call of View.
func (mr *MockConnectionKepperMockRecorder) View() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "View", reflect.TypeOf((*MockConnectionKepper)(nil).View))
}

// MockPool is a mock of Pool interface.
type MockPool struct {
	ctrl     *gomock.Controller
	recorder *MockPoolMockRecorder
	isgomock struct{}
}

// MockPoolMockRecorder is the mock recorder for MockPool.
type MockPoolMockRecorder struct {
	mock *MockPool
}

// NewMockPool creates a new mock instance.
func NewMockPool(ctrl *gomock.Controller) *MockPool {
	mock := &MockPool{ctrl: ctrl}
	mock.recorder = &MockPoolMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPool) EXPECT() *MockPoolMockRecorder {
	return m.recorder
}

// Connection mocks base method.
func (m *MockPool) Connection(clid uint, shardKey kr.ShardKey) (shard.ShardHostInstance, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Connection", clid, shardKey)
	ret0, _ := ret[0].(shard.ShardHostInstance)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Connection indicates an expected call of Connection.
func (mr *MockPoolMockRecorder) Connection(clid, shardKey any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Connection", reflect.TypeOf((*MockPool)(nil).Connection), clid, shardKey)
}

// Discard mocks base method.
func (m *MockPool) Discard(sh shard.ShardHostInstance) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Discard", sh)
	ret0, _ := ret[0].(error)
	return ret0
}

// Discard indicates an expected call of Discard.
func (mr *MockPoolMockRecorder) Discard(sh any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Discard", reflect.TypeOf((*MockPool)(nil).Discard), sh)
}

// ForEach mocks base method.
func (m *MockPool) ForEach(cb func(shard.ShardHostInfo) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ForEach", cb)
	ret0, _ := ret[0].(error)
	return ret0
}

// ForEach indicates an expected call of ForEach.
func (mr *MockPoolMockRecorder) ForEach(cb any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ForEach", reflect.TypeOf((*MockPool)(nil).ForEach), cb)
}

// Put mocks base method.
func (m *MockPool) Put(host shard.ShardHostInstance) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Put", host)
	ret0, _ := ret[0].(error)
	return ret0
}

// Put indicates an expected call of Put.
func (mr *MockPoolMockRecorder) Put(host any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Put", reflect.TypeOf((*MockPool)(nil).Put), host)
}

// View mocks base method.
func (m *MockPool) View() pool.Statistics {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "View")
	ret0, _ := ret[0].(pool.Statistics)
	return ret0
}

// View indicates an expected call of View.
func (mr *MockPoolMockRecorder) View() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "View", reflect.TypeOf((*MockPool)(nil).View))
}

// MockMultiShardPool is a mock of MultiShardPool interface.
type MockMultiShardPool struct {
	ctrl     *gomock.Controller
	recorder *MockMultiShardPoolMockRecorder
	isgomock struct{}
}

// MockMultiShardPoolMockRecorder is the mock recorder for MockMultiShardPool.
type MockMultiShardPoolMockRecorder struct {
	mock *MockMultiShardPool
}

// NewMockMultiShardPool creates a new mock instance.
func NewMockMultiShardPool(ctrl *gomock.Controller) *MockMultiShardPool {
	mock := &MockMultiShardPool{ctrl: ctrl}
	mock.recorder = &MockMultiShardPoolMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMultiShardPool) EXPECT() *MockMultiShardPoolMockRecorder {
	return m.recorder
}

// ConnectionHost mocks base method.
func (m *MockMultiShardPool) ConnectionHost(clid uint, shardKey kr.ShardKey, host config.Host) (shard.ShardHostInstance, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ConnectionHost", clid, shardKey, host)
	ret0, _ := ret[0].(shard.ShardHostInstance)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ConnectionHost indicates an expected call of ConnectionHost.
func (mr *MockMultiShardPoolMockRecorder) ConnectionHost(clid, shardKey, host any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ConnectionHost", reflect.TypeOf((*MockMultiShardPool)(nil).ConnectionHost), clid, shardKey, host)
}

// Discard mocks base method.
func (m *MockMultiShardPool) Discard(sh shard.ShardHostInstance) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Discard", sh)
	ret0, _ := ret[0].(error)
	return ret0
}

// Discard indicates an expected call of Discard.
func (mr *MockMultiShardPoolMockRecorder) Discard(sh any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Discard", reflect.TypeOf((*MockMultiShardPool)(nil).Discard), sh)
}

// ForEach mocks base method.
func (m *MockMultiShardPool) ForEach(cb func(shard.ShardHostInfo) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ForEach", cb)
	ret0, _ := ret[0].(error)
	return ret0
}

// ForEach indicates an expected call of ForEach.
func (mr *MockMultiShardPoolMockRecorder) ForEach(cb any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ForEach", reflect.TypeOf((*MockMultiShardPool)(nil).ForEach), cb)
}

// ForEachPool mocks base method.
func (m *MockMultiShardPool) ForEachPool(cb func(pool.Pool) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ForEachPool", cb)
	ret0, _ := ret[0].(error)
	return ret0
}

// ForEachPool indicates an expected call of ForEachPool.
func (mr *MockMultiShardPoolMockRecorder) ForEachPool(cb any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ForEachPool", reflect.TypeOf((*MockMultiShardPool)(nil).ForEachPool), cb)
}

// ID mocks base method.
func (m *MockMultiShardPool) ID() uint {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(uint)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockMultiShardPoolMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockMultiShardPool)(nil).ID))
}

// Put mocks base method.
func (m *MockMultiShardPool) Put(host shard.ShardHostInstance) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Put", host)
	ret0, _ := ret[0].(error)
	return ret0
}

// Put indicates an expected call of Put.
func (mr *MockMultiShardPoolMockRecorder) Put(host any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Put", reflect.TypeOf((*MockMultiShardPool)(nil).Put), host)
}

// SetRule mocks base method.
func (m *MockMultiShardPool) SetRule(rule *config.BackendRule) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetRule", rule)
}

// SetRule indicates an expected call of SetRule.
func (mr *MockMultiShardPoolMockRecorder) SetRule(rule any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetRule", reflect.TypeOf((*MockMultiShardPool)(nil).SetRule), rule)
}

// View mocks base method.
func (m *MockMultiShardPool) View() pool.Statistics {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "View")
	ret0, _ := ret[0].(pool.Statistics)
	return ret0
}

// View indicates an expected call of View.
func (mr *MockMultiShardPoolMockRecorder) View() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "View", reflect.TypeOf((*MockMultiShardPool)(nil).View))
}

// MockMultiShardTSAPool is a mock of MultiShardTSAPool interface.
type MockMultiShardTSAPool struct {
	ctrl     *gomock.Controller
	recorder *MockMultiShardTSAPoolMockRecorder
	isgomock struct{}
}

// MockMultiShardTSAPoolMockRecorder is the mock recorder for MockMultiShardTSAPool.
type MockMultiShardTSAPoolMockRecorder struct {
	mock *MockMultiShardTSAPool
}

// NewMockMultiShardTSAPool creates a new mock instance.
func NewMockMultiShardTSAPool(ctrl *gomock.Controller) *MockMultiShardTSAPool {
	mock := &MockMultiShardTSAPool{ctrl: ctrl}
	mock.recorder = &MockMultiShardTSAPoolMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMultiShardTSAPool) EXPECT() *MockMultiShardTSAPoolMockRecorder {
	return m.recorder
}

// ConnectionHost mocks base method.
func (m *MockMultiShardTSAPool) ConnectionHost(clid uint, shardKey kr.ShardKey, host config.Host) (shard.ShardHostInstance, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ConnectionHost", clid, shardKey, host)
	ret0, _ := ret[0].(shard.ShardHostInstance)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ConnectionHost indicates an expected call of ConnectionHost.
func (mr *MockMultiShardTSAPoolMockRecorder) ConnectionHost(clid, shardKey, host any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ConnectionHost", reflect.TypeOf((*MockMultiShardTSAPool)(nil).ConnectionHost), clid, shardKey, host)
}

// ConnectionWithTSA mocks base method.
func (m *MockMultiShardTSAPool) ConnectionWithTSA(clid uint, key kr.ShardKey, targetSessionAttrs tsa.TSA) (shard.ShardHostInstance, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ConnectionWithTSA", clid, key, targetSessionAttrs)
	ret0, _ := ret[0].(shard.ShardHostInstance)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ConnectionWithTSA indicates an expected call of ConnectionWithTSA.
func (mr *MockMultiShardTSAPoolMockRecorder) ConnectionWithTSA(clid, key, targetSessionAttrs any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ConnectionWithTSA", reflect.TypeOf((*MockMultiShardTSAPool)(nil).ConnectionWithTSA), clid, key, targetSessionAttrs)
}

// Discard mocks base method.
func (m *MockMultiShardTSAPool) Discard(sh shard.ShardHostInstance) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Discard", sh)
	ret0, _ := ret[0].(error)
	return ret0
}

// Discard indicates an expected call of Discard.
func (mr *MockMultiShardTSAPoolMockRecorder) Discard(sh any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Discard", reflect.TypeOf((*MockMultiShardTSAPool)(nil).Discard), sh)
}

// ForEach mocks base method.
func (m *MockMultiShardTSAPool) ForEach(cb func(shard.ShardHostInfo) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ForEach", cb)
	ret0, _ := ret[0].(error)
	return ret0
}

// ForEach indicates an expected call of ForEach.
func (mr *MockMultiShardTSAPoolMockRecorder) ForEach(cb any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ForEach", reflect.TypeOf((*MockMultiShardTSAPool)(nil).ForEach), cb)
}

// ForEachPool mocks base method.
func (m *MockMultiShardTSAPool) ForEachPool(cb func(pool.Pool) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ForEachPool", cb)
	ret0, _ := ret[0].(error)
	return ret0
}

// ForEachPool indicates an expected call of ForEachPool.
func (mr *MockMultiShardTSAPoolMockRecorder) ForEachPool(cb any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ForEachPool", reflect.TypeOf((*MockMultiShardTSAPool)(nil).ForEachPool), cb)
}

// ID mocks base method.
func (m *MockMultiShardTSAPool) ID() uint {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(uint)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockMultiShardTSAPoolMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockMultiShardTSAPool)(nil).ID))
}

// InstanceHealthChecks mocks base method.
func (m *MockMultiShardTSAPool) InstanceHealthChecks() map[string]tsa.CachedCheckResult {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InstanceHealthChecks")
	ret0, _ := ret[0].(map[string]tsa.CachedCheckResult)
	return ret0
}

// InstanceHealthChecks indicates an expected call of InstanceHealthChecks.
func (mr *MockMultiShardTSAPoolMockRecorder) InstanceHealthChecks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InstanceHealthChecks", reflect.TypeOf((*MockMultiShardTSAPool)(nil).InstanceHealthChecks))
}

// Put mocks base method.
func (m *MockMultiShardTSAPool) Put(host shard.ShardHostInstance) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Put", host)
	ret0, _ := ret[0].(error)
	return ret0
}

// Put indicates an expected call of Put.
func (mr *MockMultiShardTSAPoolMockRecorder) Put(host any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Put", reflect.TypeOf((*MockMultiShardTSAPool)(nil).Put), host)
}

// SetRule mocks base method.
func (m *MockMultiShardTSAPool) SetRule(rule *config.BackendRule) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetRule", rule)
}

// SetRule indicates an expected call of SetRule.
func (mr *MockMultiShardTSAPoolMockRecorder) SetRule(rule any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetRule", reflect.TypeOf((*MockMultiShardTSAPool)(nil).SetRule), rule)
}

// ShardMapping mocks base method.
func (m *MockMultiShardTSAPool) ShardMapping() map[string]*config.Shard {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ShardMapping")
	ret0, _ := ret[0].(map[string]*config.Shard)
	return ret0
}

// ShardMapping indicates an expected call of ShardMapping.
func (mr *MockMultiShardTSAPoolMockRecorder) ShardMapping() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ShardMapping", reflect.TypeOf((*MockMultiShardTSAPool)(nil).ShardMapping))
}

// StopCacheWatchdog mocks base method.
func (m *MockMultiShardTSAPool) StopCacheWatchdog() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "StopCacheWatchdog")
}

// StopCacheWatchdog indicates an expected call of StopCacheWatchdog.
func (mr *MockMultiShardTSAPoolMockRecorder) StopCacheWatchdog() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StopCacheWatchdog", reflect.TypeOf((*MockMultiShardTSAPool)(nil).StopCacheWatchdog))
}

// View mocks base method.
func (m *MockMultiShardTSAPool) View() pool.Statistics {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "View")
	ret0, _ := ret[0].(pool.Statistics)
	return ret0
}

// View indicates an expected call of View.
func (mr *MockMultiShardTSAPoolMockRecorder) View() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "View", reflect.TypeOf((*MockMultiShardTSAPool)(nil).View))
}

// MockPoolIterator is a mock of PoolIterator interface.
type MockPoolIterator struct {
	ctrl     *gomock.Controller
	recorder *MockPoolIteratorMockRecorder
	isgomock struct{}
}

// MockPoolIteratorMockRecorder is the mock recorder for MockPoolIterator.
type MockPoolIteratorMockRecorder struct {
	mock *MockPoolIterator
}

// NewMockPoolIterator creates a new mock instance.
func NewMockPoolIterator(ctrl *gomock.Controller) *MockPoolIterator {
	mock := &MockPoolIterator{ctrl: ctrl}
	mock.recorder = &MockPoolIteratorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPoolIterator) EXPECT() *MockPoolIteratorMockRecorder {
	return m.recorder
}

// ForEachPool mocks base method.
func (m *MockPoolIterator) ForEachPool(cb func(pool.Pool) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ForEachPool", cb)
	ret0, _ := ret[0].(error)
	return ret0
}

// ForEachPool indicates an expected call of ForEachPool.
func (mr *MockPoolIteratorMockRecorder) ForEachPool(cb any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ForEachPool", reflect.TypeOf((*MockPoolIterator)(nil).ForEachPool), cb)
}
