package shard

import (
	"crypto/tls"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

type mockShard struct {
	id uint
}

func (m *mockShard) ID() uint                                  { return m.id }
func (m *mockShard) ShardKeyName() string                      { return "" }
func (m *mockShard) InstanceHostname() string                  { return "" }
func (m *mockShard) Usr() string                               { return "" }
func (m *mockShard) DB() string                                { return "" }
func (m *mockShard) Sync() int64                               { return 0 }
func (m *mockShard) TxServed() int64                           { return 0 }
func (m *mockShard) Cfg() *config.Shard                        { return nil }
func (m *mockShard) Name() string                              { return "" }
func (m *mockShard) SHKey() kr.ShardKey                        { return kr.ShardKey{} }
func (m *mockShard) Send(query pgproto3.FrontendMessage) error { return nil }
func (m *mockShard) Receive() (pgproto3.BackendMessage, error) { return nil, nil }
func (m *mockShard) AddTLSConf(cfg *tls.Config) error          { return nil }
func (m *mockShard) Cleanup(rule *config.FrontendRule) error   { return nil }
func (m *mockShard) ConstructSM() *pgproto3.StartupMessage     { return nil }
func (m *mockShard) Instance() conn.DBInstance                 { return nil }
func (m *mockShard) Cancel() error                             { return nil }
func (m *mockShard) Params() ParameterSet                      { return nil }
func (m *mockShard) Close() error                              { return nil }
func (m *mockShard) SetTxStatus(status txstatus.TXStatus)      {}
func (m *mockShard) TxStatus() txstatus.TXStatus               { return txstatus.TXStatus(0) }
func (m *mockShard) HasPrepareStatement(hash uint64) (bool, PreparedStatementDescriptor) {
	return false, PreparedStatementDescriptor{}
}
func (m *mockShard) PrepareStatement(hash uint64, rd PreparedStatementDescriptor) {}
