package shard

import (
	"crypto/tls"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

type ParameterStatus struct {
	Name  string
	Value string
}

type ParameterSet map[string]string

// TODO : unit tests
func (ps ParameterSet) Save(status ParameterStatus) bool {
	if _, ok := ps[status.Name]; ok {
		return false
	}
	ps[status.Name] = status.Value
	return true
}

type Shardinfo interface {
	ID() uint
	ShardKeyName() string
	InstanceHostname() string
	Usr() string
	DB() string
	Sync() int64
	TxServed() int64
	TxStatus() txstatus.TXStatus
}

type CoordShardinfo interface {
	Shardinfo
	Router() string
}

type PreparedStatementDescriptor struct {
	NoData    bool
	ParamDesc pgproto3.ParameterDescription
	RowDesc   pgproto3.RowDescription
}

type PreparedStatementHolder interface {
	HasPrepareStatement(hash uint64) (bool, PreparedStatementDescriptor)
	PrepareStatement(hash uint64, rd PreparedStatementDescriptor)
}

type Shard interface {
	txstatus.TxStatusMgr
	PreparedStatementHolder
	Shardinfo

	Cfg() *config.Shard

	Name() string
	SHKey() kr.ShardKey
	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	AddTLSConf(cfg *tls.Config) error
	Cleanup(rule *config.FrontendRule) error

	ConstructSM() *pgproto3.StartupMessage
	Instance() conn.DBInstance

	Cancel() error

	Params() ParameterSet
	Close() error
}

type ShardIterator interface {
	ForEach(cb func(sh Shardinfo) error) error
}
