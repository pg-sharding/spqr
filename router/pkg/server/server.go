package server

import (
	"crypto/tls"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/models/kr"
)

type Server interface {
	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	AddShard(shkey kr.ShardKey) error
	UnrouteShard(sh kr.ShardKey) error

	AddTLSConf(cfg *tls.Config) error

	Cleanup() error
	Reset() error
}
