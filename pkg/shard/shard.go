package shard

import (
	"fmt"
	"time"

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

type ShardHostCtl interface {
	ID() uint
	ShardKeyName() string
	InstanceHostname() string

	CreatedAt() time.Time

	Pid() uint32
	Usr() string
	DB() string
	Sync() int64
	DataPending() bool

	TxServed() int64
	TxStatus() txstatus.TXStatus

	MarkStale()
	IsStale() bool
	Cancel() error

	ListPreparedStatements() []PreparedStatementsMgrDescriptor
}

type CoordShardinfo interface {
	ShardHostCtl
	Router() string
}

type ShardHostInstance interface {
	txstatus.TxStatusMgr
	prepstatement.PreparedStatementHolder
	ShardHostCtl

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

type ShardHostIterator interface {
	ForEach(cb func(sh ShardHostCtl) error) error
}

/* util function to deploy begin on shard. Used by executor and tx expand and 2pc commit. */
func DeployTxOnShard(sh ShardHostInstance, qry pgproto3.FrontendMessage, expTx txstatus.TXStatus) (txstatus.TXStatus, error) {
	if err := sh.Send(qry); err != nil {
		return txstatus.TXERR, err
	}
	/* we expect command complete and rfq as begin response */
	msg, err := sh.Receive()
	if err != nil {
		return txstatus.TXERR, err
	}
	if _, ok := msg.(*pgproto3.CommandComplete); !ok {
		return txstatus.TXERR, fmt.Errorf("unexpected response in transaction deploy %+v", msg)
	}

	/* we expect command complete and rfq as begin response */
	msg, err = sh.Receive()
	if err != nil {
		return txstatus.TXERR, err
	}
	q, ok := msg.(*pgproto3.ReadyForQuery)
	if !ok || q.TxStatus != byte(expTx) {
		return txstatus.TXERR, fmt.Errorf("unexpected response in transaction deploy %+v", msg)
	}
	return txstatus.TXStatus(q.TxStatus), nil
}
