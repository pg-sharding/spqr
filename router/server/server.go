package server

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/planopts"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

type Server interface {
	prepstatement.PreparedStatementHolder
	txstatus.TxStatusMgr

	Name() string
	Send(query pgproto3.FrontendMessage) error

	SendShard(query pgproto3.FrontendMessage, shKey kr.ShardKey) error

	/* XXX: remove two methods below and derive next message in executor */
	/* message, gang source index, error */
	Receive(o *planopts.PlanOpts) (pgproto3.BackendMessage, uint, error)
	ReceiveShard(shardId uint) (pgproto3.BackendMessage, error)

	/* TODO: add and support gang id here. */
	AllocateGangMember(params pool.ConnAllocParams, shardKey kr.ShardKey) error
	ExpandGang(params pool.ConnAllocParams, shkey kr.ShardKey, deployTX bool) error

	ToMultishard() Server

	UnRouteShard(sh kr.ShardKey) (shard.ShardHostInstance, error)
	Datashards() []shard.ShardHostInstance
	PrefetchResult(shkey kr.ShardKey, syncCnt uint) error
	PrefetchUntilCommandComplete(shkey kr.ShardKey) error

	Cancel() error
	CancellableIDs() []uint32

	Reset() error
	Sync() int64

	DataPending() bool
}

func ServerShkeys(s Server) []kr.ShardKey {
	ret := []kr.ShardKey{}
	for _, sh := range s.Datashards() {
		k := sh.SHKey()
		ret = append(ret, k)
	}
	return ret
}
