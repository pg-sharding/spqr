// Code generated by MockGen. DO NOT EDIT.
// Source: ./router/client/client.go

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	tls "crypto/tls"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	pgproto3 "github.com/jackc/pgx/v5/pgproto3"
	config "github.com/pg-sharding/spqr/pkg/config"
	shard "github.com/pg-sharding/spqr/pkg/shard"
	txstatus "github.com/pg-sharding/spqr/pkg/txstatus"
	route "github.com/pg-sharding/spqr/router/route"
	server "github.com/pg-sharding/spqr/router/server"
)

// MockPreparedStatementMapper is a mock of PreparedStatementMapper interface.
type MockPreparedStatementMapper struct {
	ctrl     *gomock.Controller
	recorder *MockPreparedStatementMapperMockRecorder
}

// MockPreparedStatementMapperMockRecorder is the mock recorder for MockPreparedStatementMapper.
type MockPreparedStatementMapperMockRecorder struct {
	mock *MockPreparedStatementMapper
}

// NewMockPreparedStatementMapper creates a new mock instance.
func NewMockPreparedStatementMapper(ctrl *gomock.Controller) *MockPreparedStatementMapper {
	mock := &MockPreparedStatementMapper{ctrl: ctrl}
	mock.recorder = &MockPreparedStatementMapperMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPreparedStatementMapper) EXPECT() *MockPreparedStatementMapperMockRecorder {
	return m.recorder
}

// PreparedStatementQueryByName mocks base method.
func (m *MockPreparedStatementMapper) PreparedStatementQueryByName(name string) string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PreparedStatementQueryByName", name)
	ret0, _ := ret[0].(string)
	return ret0
}

// PreparedStatementQueryByName indicates an expected call of PreparedStatementQueryByName.
func (mr *MockPreparedStatementMapperMockRecorder) PreparedStatementQueryByName(name interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PreparedStatementQueryByName", reflect.TypeOf((*MockPreparedStatementMapper)(nil).PreparedStatementQueryByName), name)
}

// StorePreparedStatement mocks base method.
func (m *MockPreparedStatementMapper) StorePreparedStatement(name, query string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "StorePreparedStatement", name, query)
}

// StorePreparedStatement indicates an expected call of StorePreparedStatement.
func (mr *MockPreparedStatementMapperMockRecorder) StorePreparedStatement(name, query interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StorePreparedStatement", reflect.TypeOf((*MockPreparedStatementMapper)(nil).StorePreparedStatement), name, query)
}

// MockRouterClient is a mock of RouterClient interface.
type MockRouterClient struct {
	ctrl     *gomock.Controller
	recorder *MockRouterClientMockRecorder
}

// MockRouterClientMockRecorder is the mock recorder for MockRouterClient.
type MockRouterClientMockRecorder struct {
	mock *MockRouterClient
}

// NewMockRouterClient creates a new mock instance.
func NewMockRouterClient(ctrl *gomock.Controller) *MockRouterClient {
	mock := &MockRouterClient{ctrl: ctrl}
	mock.recorder = &MockRouterClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRouterClient) EXPECT() *MockRouterClientMockRecorder {
	return m.recorder
}

// AssignRoute mocks base method.
func (m *MockRouterClient) AssignRoute(r *route.Route) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AssignRoute", r)
	ret0, _ := ret[0].(error)
	return ret0
}

// AssignRoute indicates an expected call of AssignRoute.
func (mr *MockRouterClientMockRecorder) AssignRoute(r interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AssignRoute", reflect.TypeOf((*MockRouterClient)(nil).AssignRoute), r)
}

// AssignRule mocks base method.
func (m *MockRouterClient) AssignRule(rule *config.FrontendRule) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AssignRule", rule)
	ret0, _ := ret[0].(error)
	return ret0
}

// AssignRule indicates an expected call of AssignRule.
func (mr *MockRouterClientMockRecorder) AssignRule(rule interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AssignRule", reflect.TypeOf((*MockRouterClient)(nil).AssignRule), rule)
}

// AssignServerConn mocks base method.
func (m *MockRouterClient) AssignServerConn(srv server.Server) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AssignServerConn", srv)
	ret0, _ := ret[0].(error)
	return ret0
}

// AssignServerConn indicates an expected call of AssignServerConn.
func (mr *MockRouterClientMockRecorder) AssignServerConn(srv interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AssignServerConn", reflect.TypeOf((*MockRouterClient)(nil).AssignServerConn), srv)
}

// Auth mocks base method.
func (m *MockRouterClient) Auth(rt *route.Route) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Auth", rt)
	ret0, _ := ret[0].(error)
	return ret0
}

// Auth indicates an expected call of Auth.
func (mr *MockRouterClientMockRecorder) Auth(rt interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Auth", reflect.TypeOf((*MockRouterClient)(nil).Auth), rt)
}

// Cancel mocks base method.
func (m *MockRouterClient) Cancel() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Cancel")
	ret0, _ := ret[0].(error)
	return ret0
}

// Cancel indicates an expected call of Cancel.
func (mr *MockRouterClientMockRecorder) Cancel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cancel", reflect.TypeOf((*MockRouterClient)(nil).Cancel))
}

// CancelMsg mocks base method.
func (m *MockRouterClient) CancelMsg() *pgproto3.CancelRequest {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CancelMsg")
	ret0, _ := ret[0].(*pgproto3.CancelRequest)
	return ret0
}

// CancelMsg indicates an expected call of CancelMsg.
func (mr *MockRouterClientMockRecorder) CancelMsg() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CancelMsg", reflect.TypeOf((*MockRouterClient)(nil).CancelMsg))
}

// Close mocks base method.
func (m *MockRouterClient) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockRouterClientMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockRouterClient)(nil).Close))
}

// CommitActiveSet mocks base method.
func (m *MockRouterClient) CommitActiveSet() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "CommitActiveSet")
}

// CommitActiveSet indicates an expected call of CommitActiveSet.
func (mr *MockRouterClientMockRecorder) CommitActiveSet() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CommitActiveSet", reflect.TypeOf((*MockRouterClient)(nil).CommitActiveSet))
}

// ConstructClientParams mocks base method.
func (m *MockRouterClient) ConstructClientParams() *pgproto3.Query {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ConstructClientParams")
	ret0, _ := ret[0].(*pgproto3.Query)
	return ret0
}

// ConstructClientParams indicates an expected call of ConstructClientParams.
func (mr *MockRouterClientMockRecorder) ConstructClientParams() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ConstructClientParams", reflect.TypeOf((*MockRouterClient)(nil).ConstructClientParams))
}

// DB mocks base method.
func (m *MockRouterClient) DB() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DB")
	ret0, _ := ret[0].(string)
	return ret0
}

// DB indicates an expected call of DB.
func (mr *MockRouterClientMockRecorder) DB() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DB", reflect.TypeOf((*MockRouterClient)(nil).DB))
}

// DS mocks base method.
func (m *MockRouterClient) DS() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DS")
	ret0, _ := ret[0].(string)
	return ret0
}

// DS indicates an expected call of DS.
func (mr *MockRouterClientMockRecorder) DS() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DS", reflect.TypeOf((*MockRouterClient)(nil).DS))
}

// DefaultReply mocks base method.
func (m *MockRouterClient) DefaultReply() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DefaultReply")
	ret0, _ := ret[0].(error)
	return ret0
}

// DefaultReply indicates an expected call of DefaultReply.
func (mr *MockRouterClientMockRecorder) DefaultReply() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DefaultReply", reflect.TypeOf((*MockRouterClient)(nil).DefaultReply))
}

// FireMsg mocks base method.
func (m *MockRouterClient) FireMsg(query pgproto3.FrontendMessage) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FireMsg", query)
	ret0, _ := ret[0].(error)
	return ret0
}

// FireMsg indicates an expected call of FireMsg.
func (mr *MockRouterClientMockRecorder) FireMsg(query interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FireMsg", reflect.TypeOf((*MockRouterClient)(nil).FireMsg), query)
}

// GetCancelKey mocks base method.
func (m *MockRouterClient) GetCancelKey() uint32 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCancelKey")
	ret0, _ := ret[0].(uint32)
	return ret0
}

// GetCancelKey indicates an expected call of GetCancelKey.
func (mr *MockRouterClientMockRecorder) GetCancelKey() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCancelKey", reflect.TypeOf((*MockRouterClient)(nil).GetCancelKey))
}

// GetCancelPid mocks base method.
func (m *MockRouterClient) GetCancelPid() uint32 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCancelPid")
	ret0, _ := ret[0].(uint32)
	return ret0
}

// GetCancelPid indicates an expected call of GetCancelPid.
func (mr *MockRouterClientMockRecorder) GetCancelPid() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCancelPid", reflect.TypeOf((*MockRouterClient)(nil).GetCancelPid))
}

// GetTsa mocks base method.
func (m *MockRouterClient) GetTsa() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTsa")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetTsa indicates an expected call of GetTsa.
func (mr *MockRouterClientMockRecorder) GetTsa() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTsa", reflect.TypeOf((*MockRouterClient)(nil).GetTsa))
}

// ID mocks base method.
func (m *MockRouterClient) ID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockRouterClientMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockRouterClient)(nil).ID))
}

// Init mocks base method.
func (m *MockRouterClient) Init(cfg *tls.Config) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", cfg)
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockRouterClientMockRecorder) Init(cfg interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*MockRouterClient)(nil).Init), cfg)
}

// Params mocks base method.
func (m *MockRouterClient) Params() map[string]string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Params")
	ret0, _ := ret[0].(map[string]string)
	return ret0
}

// Params indicates an expected call of Params.
func (mr *MockRouterClientMockRecorder) Params() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Params", reflect.TypeOf((*MockRouterClient)(nil).Params))
}

// PasswordCT mocks base method.
func (m *MockRouterClient) PasswordCT() (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PasswordCT")
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PasswordCT indicates an expected call of PasswordCT.
func (mr *MockRouterClientMockRecorder) PasswordCT() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PasswordCT", reflect.TypeOf((*MockRouterClient)(nil).PasswordCT))
}

// PasswordMD5 mocks base method.
func (m *MockRouterClient) PasswordMD5(salt [4]byte) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PasswordMD5", salt)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PasswordMD5 indicates an expected call of PasswordMD5.
func (mr *MockRouterClientMockRecorder) PasswordMD5(salt interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PasswordMD5", reflect.TypeOf((*MockRouterClient)(nil).PasswordMD5), salt)
}

// PreparedStatementQueryByName mocks base method.
func (m *MockRouterClient) PreparedStatementQueryByName(name string) string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PreparedStatementQueryByName", name)
	ret0, _ := ret[0].(string)
	return ret0
}

// PreparedStatementQueryByName indicates an expected call of PreparedStatementQueryByName.
func (mr *MockRouterClientMockRecorder) PreparedStatementQueryByName(name interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PreparedStatementQueryByName", reflect.TypeOf((*MockRouterClient)(nil).PreparedStatementQueryByName), name)
}

// RLock mocks base method.
func (m *MockRouterClient) RLock() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RLock")
}

// RLock indicates an expected call of RLock.
func (mr *MockRouterClientMockRecorder) RLock() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RLock", reflect.TypeOf((*MockRouterClient)(nil).RLock))
}

// RUnlock mocks base method.
func (m *MockRouterClient) RUnlock() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RUnlock")
}

// RUnlock indicates an expected call of RUnlock.
func (mr *MockRouterClientMockRecorder) RUnlock() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RUnlock", reflect.TypeOf((*MockRouterClient)(nil).RUnlock))
}

// Receive mocks base method.
func (m *MockRouterClient) Receive() (pgproto3.FrontendMessage, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Receive")
	ret0, _ := ret[0].(pgproto3.FrontendMessage)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Receive indicates an expected call of Receive.
func (mr *MockRouterClientMockRecorder) Receive() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Receive", reflect.TypeOf((*MockRouterClient)(nil).Receive))
}

// ReceiveCtx mocks base method.
func (m *MockRouterClient) ReceiveCtx(ctx context.Context) (pgproto3.FrontendMessage, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReceiveCtx", ctx)
	ret0, _ := ret[0].(pgproto3.FrontendMessage)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReceiveCtx indicates an expected call of ReceiveCtx.
func (mr *MockRouterClientMockRecorder) ReceiveCtx(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReceiveCtx", reflect.TypeOf((*MockRouterClient)(nil).ReceiveCtx), ctx)
}

// Reply mocks base method.
func (m *MockRouterClient) Reply(msg string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Reply", msg)
	ret0, _ := ret[0].(error)
	return ret0
}

// Reply indicates an expected call of Reply.
func (mr *MockRouterClientMockRecorder) Reply(msg interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Reply", reflect.TypeOf((*MockRouterClient)(nil).Reply), msg)
}

// ReplyCommandComplete mocks base method.
func (m *MockRouterClient) ReplyCommandComplete(st txstatus.TXStatus, commandTag string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplyCommandComplete", st, commandTag)
	ret0, _ := ret[0].(error)
	return ret0
}

// ReplyCommandComplete indicates an expected call of ReplyCommandComplete.
func (mr *MockRouterClientMockRecorder) ReplyCommandComplete(st, commandTag interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplyCommandComplete", reflect.TypeOf((*MockRouterClient)(nil).ReplyCommandComplete), st, commandTag)
}

// ReplyDebugNotice mocks base method.
func (m *MockRouterClient) ReplyDebugNotice(msg string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplyDebugNotice", msg)
	ret0, _ := ret[0].(error)
	return ret0
}

// ReplyDebugNotice indicates an expected call of ReplyDebugNotice.
func (mr *MockRouterClientMockRecorder) ReplyDebugNotice(msg interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplyDebugNotice", reflect.TypeOf((*MockRouterClient)(nil).ReplyDebugNotice), msg)
}

// ReplyDebugNoticef mocks base method.
func (m *MockRouterClient) ReplyDebugNoticef(fmt string, args ...interface{}) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{fmt}
	for _, a := range args {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ReplyDebugNoticef", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// ReplyDebugNoticef indicates an expected call of ReplyDebugNoticef.
func (mr *MockRouterClientMockRecorder) ReplyDebugNoticef(fmt interface{}, args ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{fmt}, args...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplyDebugNoticef", reflect.TypeOf((*MockRouterClient)(nil).ReplyDebugNoticef), varargs...)
}

// ReplyErrMsg mocks base method.
func (m *MockRouterClient) ReplyErrMsg(errmsg string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplyErrMsg", errmsg)
	ret0, _ := ret[0].(error)
	return ret0
}

// ReplyErrMsg indicates an expected call of ReplyErrMsg.
func (mr *MockRouterClientMockRecorder) ReplyErrMsg(errmsg interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplyErrMsg", reflect.TypeOf((*MockRouterClient)(nil).ReplyErrMsg), errmsg)
}

// ReplyNotice mocks base method.
func (m *MockRouterClient) ReplyNotice(message string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplyNotice", message)
	ret0, _ := ret[0].(error)
	return ret0
}

// ReplyNotice indicates an expected call of ReplyNotice.
func (mr *MockRouterClientMockRecorder) ReplyNotice(message interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplyNotice", reflect.TypeOf((*MockRouterClient)(nil).ReplyNotice), message)
}

// ReplyParseComplete mocks base method.
func (m *MockRouterClient) ReplyParseComplete() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplyParseComplete")
	ret0, _ := ret[0].(error)
	return ret0
}

// ReplyParseComplete indicates an expected call of ReplyParseComplete.
func (mr *MockRouterClientMockRecorder) ReplyParseComplete() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplyParseComplete", reflect.TypeOf((*MockRouterClient)(nil).ReplyParseComplete))
}

// ReplyRFQ mocks base method.
func (m *MockRouterClient) ReplyRFQ() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplyRFQ")
	ret0, _ := ret[0].(error)
	return ret0
}

// ReplyRFQ indicates an expected call of ReplyRFQ.
func (mr *MockRouterClientMockRecorder) ReplyRFQ() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplyRFQ", reflect.TypeOf((*MockRouterClient)(nil).ReplyRFQ))
}

// ReplyWarningMsg mocks base method.
func (m *MockRouterClient) ReplyWarningMsg(msg string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplyWarningMsg", msg)
	ret0, _ := ret[0].(error)
	return ret0
}

// ReplyWarningMsg indicates an expected call of ReplyWarningMsg.
func (mr *MockRouterClientMockRecorder) ReplyWarningMsg(msg interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplyWarningMsg", reflect.TypeOf((*MockRouterClient)(nil).ReplyWarningMsg), msg)
}

// ReplyWarningf mocks base method.
func (m *MockRouterClient) ReplyWarningf(fmt string, args ...interface{}) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{fmt}
	for _, a := range args {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ReplyWarningf", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// ReplyWarningf indicates an expected call of ReplyWarningf.
func (mr *MockRouterClientMockRecorder) ReplyWarningf(fmt interface{}, args ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{fmt}, args...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplyWarningf", reflect.TypeOf((*MockRouterClient)(nil).ReplyWarningf), varargs...)
}

// Reset mocks base method.
func (m *MockRouterClient) Reset() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Reset")
	ret0, _ := ret[0].(error)
	return ret0
}

// Reset indicates an expected call of Reset.
func (mr *MockRouterClientMockRecorder) Reset() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Reset", reflect.TypeOf((*MockRouterClient)(nil).Reset))
}

// ResetAll mocks base method.
func (m *MockRouterClient) ResetAll() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ResetAll")
}

// ResetAll indicates an expected call of ResetAll.
func (mr *MockRouterClientMockRecorder) ResetAll() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResetAll", reflect.TypeOf((*MockRouterClient)(nil).ResetAll))
}

// ResetParam mocks base method.
func (m *MockRouterClient) ResetParam(arg0 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ResetParam", arg0)
}

// ResetParam indicates an expected call of ResetParam.
func (mr *MockRouterClientMockRecorder) ResetParam(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResetParam", reflect.TypeOf((*MockRouterClient)(nil).ResetParam), arg0)
}

// Rollback mocks base method.
func (m *MockRouterClient) Rollback() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Rollback")
}

// Rollback indicates an expected call of Rollback.
func (mr *MockRouterClientMockRecorder) Rollback() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rollback", reflect.TypeOf((*MockRouterClient)(nil).Rollback))
}

// RollbackToSP mocks base method.
func (m *MockRouterClient) RollbackToSP(arg0 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RollbackToSP", arg0)
}

// RollbackToSP indicates an expected call of RollbackToSP.
func (mr *MockRouterClientMockRecorder) RollbackToSP(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RollbackToSP", reflect.TypeOf((*MockRouterClient)(nil).RollbackToSP), arg0)
}

// Route mocks base method.
func (m *MockRouterClient) Route() *route.Route {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Route")
	ret0, _ := ret[0].(*route.Route)
	return ret0
}

// Route indicates an expected call of Route.
func (mr *MockRouterClientMockRecorder) Route() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Route", reflect.TypeOf((*MockRouterClient)(nil).Route))
}

// Rule mocks base method.
func (m *MockRouterClient) Rule() *config.FrontendRule {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rule")
	ret0, _ := ret[0].(*config.FrontendRule)
	return ret0
}

// Rule indicates an expected call of Rule.
func (mr *MockRouterClientMockRecorder) Rule() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rule", reflect.TypeOf((*MockRouterClient)(nil).Rule))
}

// Savepoint mocks base method.
func (m *MockRouterClient) Savepoint(arg0 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Savepoint", arg0)
}

// Savepoint indicates an expected call of Savepoint.
func (mr *MockRouterClientMockRecorder) Savepoint(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Savepoint", reflect.TypeOf((*MockRouterClient)(nil).Savepoint), arg0)
}

// Send mocks base method.
func (m *MockRouterClient) Send(msg pgproto3.BackendMessage) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Send", msg)
	ret0, _ := ret[0].(error)
	return ret0
}

// Send indicates an expected call of Send.
func (mr *MockRouterClientMockRecorder) Send(msg interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockRouterClient)(nil).Send), msg)
}

// SendCtx mocks base method.
func (m *MockRouterClient) SendCtx(ctx context.Context, msg pgproto3.BackendMessage) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendCtx", ctx, msg)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendCtx indicates an expected call of SendCtx.
func (mr *MockRouterClientMockRecorder) SendCtx(ctx, msg interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendCtx", reflect.TypeOf((*MockRouterClient)(nil).SendCtx), ctx, msg)
}

// Server mocks base method.
func (m *MockRouterClient) Server() server.Server {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Server")
	ret0, _ := ret[0].(server.Server)
	return ret0
}

// Server indicates an expected call of Server.
func (mr *MockRouterClientMockRecorder) Server() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Server", reflect.TypeOf((*MockRouterClient)(nil).Server))
}

// ServerAcquireUse mocks base method.
func (m *MockRouterClient) ServerAcquireUse() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ServerAcquireUse")
}

// ServerAcquireUse indicates an expected call of ServerAcquireUse.
func (mr *MockRouterClientMockRecorder) ServerAcquireUse() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ServerAcquireUse", reflect.TypeOf((*MockRouterClient)(nil).ServerAcquireUse))
}

// ServerReleaseUse mocks base method.
func (m *MockRouterClient) ServerReleaseUse() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ServerReleaseUse")
}

// ServerReleaseUse indicates an expected call of ServerReleaseUse.
func (mr *MockRouterClientMockRecorder) ServerReleaseUse() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ServerReleaseUse", reflect.TypeOf((*MockRouterClient)(nil).ServerReleaseUse))
}

// SetAuthType mocks base method.
func (m *MockRouterClient) SetAuthType(arg0 uint32) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetAuthType", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetAuthType indicates an expected call of SetAuthType.
func (mr *MockRouterClientMockRecorder) SetAuthType(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetAuthType", reflect.TypeOf((*MockRouterClient)(nil).SetAuthType), arg0)
}

// SetDS mocks base method.
func (m *MockRouterClient) SetDS(dataspace string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetDS", dataspace)
}

// SetDS indicates an expected call of SetDS.
func (mr *MockRouterClientMockRecorder) SetDS(dataspace interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetDS", reflect.TypeOf((*MockRouterClient)(nil).SetDS), dataspace)
}

// SetParam mocks base method.
func (m *MockRouterClient) SetParam(arg0, arg1 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetParam", arg0, arg1)
}

// SetParam indicates an expected call of SetParam.
func (mr *MockRouterClientMockRecorder) SetParam(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetParam", reflect.TypeOf((*MockRouterClient)(nil).SetParam), arg0, arg1)
}

// SetTsa mocks base method.
func (m *MockRouterClient) SetTsa(arg0 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetTsa", arg0)
}

// SetTsa indicates an expected call of SetTsa.
func (mr *MockRouterClientMockRecorder) SetTsa(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetTsa", reflect.TypeOf((*MockRouterClient)(nil).SetTsa), arg0)
}

// Shards mocks base method.
func (m *MockRouterClient) Shards() []shard.Shard {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Shards")
	ret0, _ := ret[0].([]shard.Shard)
	return ret0
}

// Shards indicates an expected call of Shards.
func (mr *MockRouterClientMockRecorder) Shards() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Shards", reflect.TypeOf((*MockRouterClient)(nil).Shards))
}

// Shutdown mocks base method.
func (m *MockRouterClient) Shutdown() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Shutdown")
	ret0, _ := ret[0].(error)
	return ret0
}

// Shutdown indicates an expected call of Shutdown.
func (mr *MockRouterClientMockRecorder) Shutdown() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Shutdown", reflect.TypeOf((*MockRouterClient)(nil).Shutdown))
}

// StartTx mocks base method.
func (m *MockRouterClient) StartTx() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "StartTx")
}

// StartTx indicates an expected call of StartTx.
func (mr *MockRouterClientMockRecorder) StartTx() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartTx", reflect.TypeOf((*MockRouterClient)(nil).StartTx))
}

// StartupMessage mocks base method.
func (m *MockRouterClient) StartupMessage() *pgproto3.StartupMessage {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StartupMessage")
	ret0, _ := ret[0].(*pgproto3.StartupMessage)
	return ret0
}

// StartupMessage indicates an expected call of StartupMessage.
func (mr *MockRouterClientMockRecorder) StartupMessage() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartupMessage", reflect.TypeOf((*MockRouterClient)(nil).StartupMessage))
}

// StorePreparedStatement mocks base method.
func (m *MockRouterClient) StorePreparedStatement(name, query string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "StorePreparedStatement", name, query)
}

// StorePreparedStatement indicates an expected call of StorePreparedStatement.
func (mr *MockRouterClientMockRecorder) StorePreparedStatement(name, query interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StorePreparedStatement", reflect.TypeOf((*MockRouterClient)(nil).StorePreparedStatement), name, query)
}

// Unroute mocks base method.
func (m *MockRouterClient) Unroute() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Unroute")
	ret0, _ := ret[0].(error)
	return ret0
}

// Unroute indicates an expected call of Unroute.
func (mr *MockRouterClientMockRecorder) Unroute() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Unroute", reflect.TypeOf((*MockRouterClient)(nil).Unroute))
}

// Usr mocks base method.
func (m *MockRouterClient) Usr() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Usr")
	ret0, _ := ret[0].(string)
	return ret0
}

// Usr indicates an expected call of Usr.
func (mr *MockRouterClientMockRecorder) Usr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Usr", reflect.TypeOf((*MockRouterClient)(nil).Usr))
}
