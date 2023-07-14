package server

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

type PrepStmtDesc struct {
	Name  string
	Query string
}

type PreparedStatementHolder interface {
	HasPrepareStatement(hash uint64) bool
	PrepareStatement(hash uint64)
}

type Server interface {
	PreparedStatementHolder
	txstatus.TxStatusMgr

	Name() string
	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	AddDataShard(clid string, shardKey kr.ShardKey, tsa string) error
	UnRouteShard(sh kr.ShardKey, rule *config.FrontendRule) error
	Datashards() []shard.Shard

	Cancel() error

	Reset() error
	Sync() int64
}
