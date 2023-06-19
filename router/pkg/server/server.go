package server

import (
	"github.com/pg-sharding/spqr/pkg/config"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/models/kr"
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

	Name() string
	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	AddDataShard(clid string, shardKey kr.ShardKey, tsa string) error
	UnRouteShard(sh kr.ShardKey, rule *config.FrontendRule) error

	Cancel() error

	Reset() error
	Sync() int
}
