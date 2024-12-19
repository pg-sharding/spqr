package shard

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

type ParameterStatus struct {
	Name  string
	Value string
}

type ParameterSet map[string]string

// Save saves the given ParameterStatus to the ParameterSet.
// It returns true if the status was successfully saved, and false if the status already exists in the set.
//
// Parameters:
//   - status: The ParameterStatus to save.
//
// Returns:
//   - bool: A boolean indicating whether the status was successfully saved.
//
// TODO : unit tests
func (ps ParameterSet) Save(status ParameterStatus) bool {
	if _, ok := ps[status.Name]; ok {
		return false
	}
	ps[status.Name] = status.Value
	return true
}

type PreparedStatementsMgrDescriptor struct {
	Name     string
	Query    string
	Hash     uint64
	ServerId uint
}

type Shardinfo interface {
	ID() uint
	ShardKeyName() string
	InstanceHostname() string
	Pid() uint32
	Usr() string
	DB() string
	Sync() int64
	DataPending() bool

	RequestData()

	TxServed() int64
	TxStatus() txstatus.TXStatus

	ListPreparedStatements() []PreparedStatementsMgrDescriptor
}

type CoordShardinfo interface {
	Shardinfo
	Router() string
}

type Shard interface {
	txstatus.TxStatusMgr
	prepstatement.PreparedStatementHolder
	Shardinfo

	Name() string
	SHKey() kr.ShardKey
	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)
	Cleanup(rule *config.FrontendRule) error
	Instance() conn.DBInstance
	Cancel() error
	Params() ParameterSet
	Close() error
}

type ShardIterator interface {
	ForEach(cb func(sh Shardinfo) error) error
}
