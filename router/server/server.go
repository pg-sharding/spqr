package server

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

type Server interface {
	prepstatement.PreparedStatementHolder
	txstatus.TxStatusMgr

	Name() string
	Send(query pgproto3.FrontendMessage) error
	SendShard(query pgproto3.FrontendMessage, shardId uint) error
	Receive() (pgproto3.BackendMessage, error)
	ReceiveShard(shardId uint) (pgproto3.BackendMessage, error)

	AddDataShard(clid uint, shardKey kr.ShardKey, tsa tsa.TSA) error
	UnRouteShard(sh kr.ShardKey, rule *config.FrontendRule) error
	Datashards() []shard.Shard

	Cancel() error

	Reset() error
	Sync() int64

	DataPending() bool
}
