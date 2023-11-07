// Code generated by MockGen. DO NOT EDIT.
// Source: ./pkg/shard/shard.go
//
// Generated by this command:
//
//	mockgen -source=./pkg/shard/shard.go -destination=pkg/mock/shard/mock_shard.go -package=mock
//
// Package mock is a generated GoMock package.
package mock

import (
	tls "crypto/tls"
	reflect "reflect"

	pgproto3 "github.com/jackc/pgx/v5/pgproto3"
	config "github.com/pg-sharding/spqr/pkg/config"
	conn "github.com/pg-sharding/spqr/pkg/conn"
	kr "github.com/pg-sharding/spqr/pkg/models/kr"
	shard "github.com/pg-sharding/spqr/pkg/shard"
	txstatus "github.com/pg-sharding/spqr/pkg/txstatus"
	gomock "go.uber.org/mock/gomock"
)

// MockShardinfo is a mock of Shardinfo interface.
type MockShardinfo struct {
	ctrl     *gomock.Controller
	recorder *MockShardinfoMockRecorder
}

// MockShardinfoMockRecorder is the mock recorder for MockShardinfo.
type MockShardinfoMockRecorder struct {
	mock *MockShardinfo
}

// NewMockShardinfo creates a new mock instance.
func NewMockShardinfo(ctrl *gomock.Controller) *MockShardinfo {
	mock := &MockShardinfo{ctrl: ctrl}
	mock.recorder = &MockShardinfoMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockShardinfo) EXPECT() *MockShardinfoMockRecorder {
	return m.recorder
}

// DB mocks base method.
func (m *MockShardinfo) DB() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DB")
	ret0, _ := ret[0].(string)
	return ret0
}

// DB indicates an expected call of DB.
func (mr *MockShardinfoMockRecorder) DB() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DB", reflect.TypeOf((*MockShardinfo)(nil).DB))
}

// ID mocks base method.
func (m *MockShardinfo) ID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockShardinfoMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockShardinfo)(nil).ID))
}

// InstanceHostname mocks base method.
func (m *MockShardinfo) InstanceHostname() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InstanceHostname")
	ret0, _ := ret[0].(string)
	return ret0
}

// InstanceHostname indicates an expected call of InstanceHostname.
func (mr *MockShardinfoMockRecorder) InstanceHostname() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InstanceHostname", reflect.TypeOf((*MockShardinfo)(nil).InstanceHostname))
}

// ShardKeyName mocks base method.
func (m *MockShardinfo) ShardKeyName() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ShardKeyName")
	ret0, _ := ret[0].(string)
	return ret0
}

// ShardKeyName indicates an expected call of ShardKeyName.
func (mr *MockShardinfoMockRecorder) ShardKeyName() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ShardKeyName", reflect.TypeOf((*MockShardinfo)(nil).ShardKeyName))
}

// Sync mocks base method.
func (m *MockShardinfo) Sync() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Sync")
	ret0, _ := ret[0].(int64)
	return ret0
}

// Sync indicates an expected call of Sync.
func (mr *MockShardinfoMockRecorder) Sync() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Sync", reflect.TypeOf((*MockShardinfo)(nil).Sync))
}

// TxServed mocks base method.
func (m *MockShardinfo) TxServed() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TxServed")
	ret0, _ := ret[0].(int64)
	return ret0
}

// TxServed indicates an expected call of TxServed.
func (mr *MockShardinfoMockRecorder) TxServed() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TxServed", reflect.TypeOf((*MockShardinfo)(nil).TxServed))
}

// TxStatus mocks base method.
func (m *MockShardinfo) TxStatus() txstatus.TXStatus {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TxStatus")
	ret0, _ := ret[0].(txstatus.TXStatus)
	return ret0
}

// TxStatus indicates an expected call of TxStatus.
func (mr *MockShardinfoMockRecorder) TxStatus() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TxStatus", reflect.TypeOf((*MockShardinfo)(nil).TxStatus))
}

// Usr mocks base method.
func (m *MockShardinfo) Usr() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Usr")
	ret0, _ := ret[0].(string)
	return ret0
}

// Usr indicates an expected call of Usr.
func (mr *MockShardinfoMockRecorder) Usr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Usr", reflect.TypeOf((*MockShardinfo)(nil).Usr))
}

// MockCoordShardinfo is a mock of CoordShardinfo interface.
type MockCoordShardinfo struct {
	ctrl     *gomock.Controller
	recorder *MockCoordShardinfoMockRecorder
}

// MockCoordShardinfoMockRecorder is the mock recorder for MockCoordShardinfo.
type MockCoordShardinfoMockRecorder struct {
	mock *MockCoordShardinfo
}

// NewMockCoordShardinfo creates a new mock instance.
func NewMockCoordShardinfo(ctrl *gomock.Controller) *MockCoordShardinfo {
	mock := &MockCoordShardinfo{ctrl: ctrl}
	mock.recorder = &MockCoordShardinfoMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCoordShardinfo) EXPECT() *MockCoordShardinfoMockRecorder {
	return m.recorder
}

// DB mocks base method.
func (m *MockCoordShardinfo) DB() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DB")
	ret0, _ := ret[0].(string)
	return ret0
}

// DB indicates an expected call of DB.
func (mr *MockCoordShardinfoMockRecorder) DB() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DB", reflect.TypeOf((*MockCoordShardinfo)(nil).DB))
}

// ID mocks base method.
func (m *MockCoordShardinfo) ID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockCoordShardinfoMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockCoordShardinfo)(nil).ID))
}

// InstanceHostname mocks base method.
func (m *MockCoordShardinfo) InstanceHostname() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InstanceHostname")
	ret0, _ := ret[0].(string)
	return ret0
}

// InstanceHostname indicates an expected call of InstanceHostname.
func (mr *MockCoordShardinfoMockRecorder) InstanceHostname() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InstanceHostname", reflect.TypeOf((*MockCoordShardinfo)(nil).InstanceHostname))
}

// Router mocks base method.
func (m *MockCoordShardinfo) Router() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Router")
	ret0, _ := ret[0].(string)
	return ret0
}

// Router indicates an expected call of Router.
func (mr *MockCoordShardinfoMockRecorder) Router() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Router", reflect.TypeOf((*MockCoordShardinfo)(nil).Router))
}

// ShardKeyName mocks base method.
func (m *MockCoordShardinfo) ShardKeyName() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ShardKeyName")
	ret0, _ := ret[0].(string)
	return ret0
}

// ShardKeyName indicates an expected call of ShardKeyName.
func (mr *MockCoordShardinfoMockRecorder) ShardKeyName() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ShardKeyName", reflect.TypeOf((*MockCoordShardinfo)(nil).ShardKeyName))
}

// Sync mocks base method.
func (m *MockCoordShardinfo) Sync() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Sync")
	ret0, _ := ret[0].(int64)
	return ret0
}

// Sync indicates an expected call of Sync.
func (mr *MockCoordShardinfoMockRecorder) Sync() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Sync", reflect.TypeOf((*MockCoordShardinfo)(nil).Sync))
}

// TxServed mocks base method.
func (m *MockCoordShardinfo) TxServed() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TxServed")
	ret0, _ := ret[0].(int64)
	return ret0
}

// TxServed indicates an expected call of TxServed.
func (mr *MockCoordShardinfoMockRecorder) TxServed() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TxServed", reflect.TypeOf((*MockCoordShardinfo)(nil).TxServed))
}

// TxStatus mocks base method.
func (m *MockCoordShardinfo) TxStatus() txstatus.TXStatus {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TxStatus")
	ret0, _ := ret[0].(txstatus.TXStatus)
	return ret0
}

// TxStatus indicates an expected call of TxStatus.
func (mr *MockCoordShardinfoMockRecorder) TxStatus() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TxStatus", reflect.TypeOf((*MockCoordShardinfo)(nil).TxStatus))
}

// Usr mocks base method.
func (m *MockCoordShardinfo) Usr() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Usr")
	ret0, _ := ret[0].(string)
	return ret0
}

// Usr indicates an expected call of Usr.
func (mr *MockCoordShardinfoMockRecorder) Usr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Usr", reflect.TypeOf((*MockCoordShardinfo)(nil).Usr))
}

// MockShard is a mock of Shard interface.
type MockShard struct {
	ctrl     *gomock.Controller
	recorder *MockShardMockRecorder
}

// MockShardMockRecorder is the mock recorder for MockShard.
type MockShardMockRecorder struct {
	mock *MockShard
}

// NewMockShard creates a new mock instance.
func NewMockShard(ctrl *gomock.Controller) *MockShard {
	mock := &MockShard{ctrl: ctrl}
	mock.recorder = &MockShardMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockShard) EXPECT() *MockShardMockRecorder {
	return m.recorder
}

// AddTLSConf mocks base method.
func (m *MockShard) AddTLSConf(cfg *tls.Config) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddTLSConf", cfg)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddTLSConf indicates an expected call of AddTLSConf.
func (mr *MockShardMockRecorder) AddTLSConf(cfg any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddTLSConf", reflect.TypeOf((*MockShard)(nil).AddTLSConf), cfg)
}

// Cancel mocks base method.
func (m *MockShard) Cancel() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Cancel")
	ret0, _ := ret[0].(error)
	return ret0
}

// Cancel indicates an expected call of Cancel.
func (mr *MockShardMockRecorder) Cancel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cancel", reflect.TypeOf((*MockShard)(nil).Cancel))
}

// Cfg mocks base method.
func (m *MockShard) Cfg() *config.Shard {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Cfg")
	ret0, _ := ret[0].(*config.Shard)
	return ret0
}

// Cfg indicates an expected call of Cfg.
func (mr *MockShardMockRecorder) Cfg() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cfg", reflect.TypeOf((*MockShard)(nil).Cfg))
}

// Cleanup mocks base method.
func (m *MockShard) Cleanup(rule *config.FrontendRule) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Cleanup", rule)
	ret0, _ := ret[0].(error)
	return ret0
}

// Cleanup indicates an expected call of Cleanup.
func (mr *MockShardMockRecorder) Cleanup(rule any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cleanup", reflect.TypeOf((*MockShard)(nil).Cleanup), rule)
}

// Close mocks base method.
func (m *MockShard) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockShardMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockShard)(nil).Close))
}

// ConstructSM mocks base method.
func (m *MockShard) ConstructSM() *pgproto3.StartupMessage {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ConstructSM")
	ret0, _ := ret[0].(*pgproto3.StartupMessage)
	return ret0
}

// ConstructSM indicates an expected call of ConstructSM.
func (mr *MockShardMockRecorder) ConstructSM() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ConstructSM", reflect.TypeOf((*MockShard)(nil).ConstructSM))
}

// DB mocks base method.
func (m *MockShard) DB() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DB")
	ret0, _ := ret[0].(string)
	return ret0
}

// DB indicates an expected call of DB.
func (mr *MockShardMockRecorder) DB() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DB", reflect.TypeOf((*MockShard)(nil).DB))
}

// ID mocks base method.
func (m *MockShard) ID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockShardMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockShard)(nil).ID))
}

// Instance mocks base method.
func (m *MockShard) Instance() conn.DBInstance {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Instance")
	ret0, _ := ret[0].(conn.DBInstance)
	return ret0
}

// Instance indicates an expected call of Instance.
func (mr *MockShardMockRecorder) Instance() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Instance", reflect.TypeOf((*MockShard)(nil).Instance))
}

// InstanceHostname mocks base method.
func (m *MockShard) InstanceHostname() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InstanceHostname")
	ret0, _ := ret[0].(string)
	return ret0
}

// InstanceHostname indicates an expected call of InstanceHostname.
func (mr *MockShardMockRecorder) InstanceHostname() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InstanceHostname", reflect.TypeOf((*MockShard)(nil).InstanceHostname))
}

// Name mocks base method.
func (m *MockShard) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockShardMockRecorder) Name() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockShard)(nil).Name))
}

// Params mocks base method.
func (m *MockShard) Params() shard.ParameterSet {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Params")
	ret0, _ := ret[0].(shard.ParameterSet)
	return ret0
}

// Params indicates an expected call of Params.
func (mr *MockShardMockRecorder) Params() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Params", reflect.TypeOf((*MockShard)(nil).Params))
}

// Receive mocks base method.
func (m *MockShard) Receive() (pgproto3.BackendMessage, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Receive")
	ret0, _ := ret[0].(pgproto3.BackendMessage)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Receive indicates an expected call of Receive.
func (mr *MockShardMockRecorder) Receive() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Receive", reflect.TypeOf((*MockShard)(nil).Receive))
}

// SHKey mocks base method.
func (m *MockShard) SHKey() kr.ShardKey {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SHKey")
	ret0, _ := ret[0].(kr.ShardKey)
	return ret0
}

// SHKey indicates an expected call of SHKey.
func (mr *MockShardMockRecorder) SHKey() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SHKey", reflect.TypeOf((*MockShard)(nil).SHKey))
}

// Send mocks base method.
func (m *MockShard) Send(query pgproto3.FrontendMessage) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Send", query)
	ret0, _ := ret[0].(error)
	return ret0
}

// Send indicates an expected call of Send.
func (mr *MockShardMockRecorder) Send(query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockShard)(nil).Send), query)
}

// SetTxStatus mocks base method.
func (m *MockShard) SetTxStatus(status txstatus.TXStatus) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetTxStatus", status)
}

// SetTxStatus indicates an expected call of SetTxStatus.
func (mr *MockShardMockRecorder) SetTxStatus(status any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetTxStatus", reflect.TypeOf((*MockShard)(nil).SetTxStatus), status)
}

// ShardKeyName mocks base method.
func (m *MockShard) ShardKeyName() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ShardKeyName")
	ret0, _ := ret[0].(string)
	return ret0
}

// ShardKeyName indicates an expected call of ShardKeyName.
func (mr *MockShardMockRecorder) ShardKeyName() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ShardKeyName", reflect.TypeOf((*MockShard)(nil).ShardKeyName))
}

// Sync mocks base method.
func (m *MockShard) Sync() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Sync")
	ret0, _ := ret[0].(int64)
	return ret0
}

// Sync indicates an expected call of Sync.
func (mr *MockShardMockRecorder) Sync() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Sync", reflect.TypeOf((*MockShard)(nil).Sync))
}

// TxServed mocks base method.
func (m *MockShard) TxServed() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TxServed")
	ret0, _ := ret[0].(int64)
	return ret0
}

// TxServed indicates an expected call of TxServed.
func (mr *MockShardMockRecorder) TxServed() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TxServed", reflect.TypeOf((*MockShard)(nil).TxServed))
}

// TxStatus mocks base method.
func (m *MockShard) TxStatus() txstatus.TXStatus {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TxStatus")
	ret0, _ := ret[0].(txstatus.TXStatus)
	return ret0
}

// TxStatus indicates an expected call of TxStatus.
func (mr *MockShardMockRecorder) TxStatus() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TxStatus", reflect.TypeOf((*MockShard)(nil).TxStatus))
}

// Usr mocks base method.
func (m *MockShard) Usr() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Usr")
	ret0, _ := ret[0].(string)
	return ret0
}

// Usr indicates an expected call of Usr.
func (mr *MockShardMockRecorder) Usr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Usr", reflect.TypeOf((*MockShard)(nil).Usr))
}

// MockShardIterator is a mock of ShardIterator interface.
type MockShardIterator struct {
	ctrl     *gomock.Controller
	recorder *MockShardIteratorMockRecorder
}

// MockShardIteratorMockRecorder is the mock recorder for MockShardIterator.
type MockShardIteratorMockRecorder struct {
	mock *MockShardIterator
}

// NewMockShardIterator creates a new mock instance.
func NewMockShardIterator(ctrl *gomock.Controller) *MockShardIterator {
	mock := &MockShardIterator{ctrl: ctrl}
	mock.recorder = &MockShardIteratorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockShardIterator) EXPECT() *MockShardIteratorMockRecorder {
	return m.recorder
}

// ForEach mocks base method.
func (m *MockShardIterator) ForEach(cb func(shard.Shardinfo) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ForEach", cb)
	ret0, _ := ret[0].(error)
	return ret0
}

// ForEach indicates an expected call of ForEach.
func (mr *MockShardIteratorMockRecorder) ForEach(cb any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ForEach", reflect.TypeOf((*MockShardIterator)(nil).ForEach), cb)
}
