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

	SendShard(query pgproto3.FrontendMessage, shKey *kr.ShardKey) error

	Receive() (pgproto3.BackendMessage, error)
	ReceiveShard(shardId uint) (pgproto3.BackendMessage, error)

	AddDataShard(clid uint, shardKey kr.ShardKey, tsa tsa.TSA) error
	ExpandDataShard(clid uint, shkey kr.ShardKey, tsa tsa.TSA, deployTX bool) error

	ToMultishard() Server

	UnRouteShard(sh kr.ShardKey, rule *config.FrontendRule) error
	Datashards() []shard.ShardHostInstance

	Cancel() error

	Reset() error
	Sync() int64

	DataPending() bool
}

func ServerShkeys(s Server) []*kr.ShardKey {
	ret := []*kr.ShardKey{}
	for _, sh := range s.Datashards() {
		k := sh.SHKey()
		ret = append(ret, &k)
	}
	return ret
}
