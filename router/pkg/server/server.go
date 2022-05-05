package server

import (
	"crypto/tls"

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

	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	AddShard(shardKey kr.ShardKey) error
	UnRouteShard(sh kr.ShardKey) error

	AddTLSConf(cfg *tls.Config) error

	Cleanup() error
	Reset() error
	Sync() int
}
