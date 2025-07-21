package server

import (
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

var ErrShardUnavailable = fmt.Errorf("shard is unavailable, try again later")

type ShardServer struct {
	pool  pool.MultiShardTSAPool
	shard atomic.Pointer[shard.ShardHostInstance]
}

// ToMultishard implements Server.
func (srv *ShardServer) ToMultishard() Server {
	return NewMultiShardServerFromShard(srv.pool, *srv.shard.Load())
}

// ExpandDataShard implements Server.
func (srv *ShardServer) ExpandDataShard(clid uint, shkey kr.ShardKey, tsa tsa.TSA, deployTX bool) error {
	return fmt.Errorf("expanding transaction on single shard server in unsupported")
}

// DataPending implements Server.
func (srv *ShardServer) DataPending() bool {
	return (*srv.shard.Load()).DataPending()
}

func NewShardServer(spool pool.MultiShardTSAPool) *ShardServer {
	return &ShardServer{
		pool:  spool,
		shard: atomic.Pointer[shard.ShardHostInstance]{},
	}
}

// TODO : unit tests
func (srv *ShardServer) HasPrepareStatement(hash uint64, shardId uint) (bool, *prepstatement.PreparedStatementDescriptor) {
	b, rd := (*srv.shard.Load()).HasPrepareStatement(hash, shardId)
	return b, rd
}

// TODO : unit tests
func (srv *ShardServer) Name() string {
	v := srv.shard.Load()
	if v == nil {
		return ""
	}
	return (*v).Name()
}

// TODO : unit tests
func (srv *ShardServer) StorePrepareStatement(hash uint64, shardId uint, def *prepstatement.PreparedStatementDefinition, rd *prepstatement.PreparedStatementDescriptor) error {
	return (*srv.shard.Load()).StorePrepareStatement(hash, shardId, def, rd)
}

// TODO : unit tests
func (srv *ShardServer) Sync() int64 {
	v := srv.shard.Load()
	if v == nil {
		return 0
	}
	return (*v).Sync()
}

// TODO : unit tests
func (srv *ShardServer) Reset() error {
	// todo there are no shard writes, so use rLock
	v := srv.shard.Load()
	if v == nil {
		return ErrShardUnavailable
	}

	err := srv.pool.Put(*v)

	srv.shard.Store(nil)

	return err
}

// TODO : unit tests
func (srv *ShardServer) UnRouteShard(shkey kr.ShardKey, rule *config.FrontendRule) error {
	v := srv.shard.Load()
	if v == nil {
		return nil
	}

	srv.shard.Store(nil)

	if (*v).SHKey().Name != shkey.Name {
		return fmt.Errorf("active datashard does not match unrouted: %v != %v", (*v).SHKey().Name, shkey.Name)
	}

	if (*v).Sync() != 0 {
		/* will automatically discard connection,
		but we will not perform cleanup, which may stuck forever */
		return srv.pool.Put((*v))
	}

	if err := srv.cleanupLockFree(*v, rule); err != nil {
		return err
	}

	if err := srv.pool.Put(*v); err != nil {
		return err
	}

	return nil
}

// TODO : unit tests
func (srv *ShardServer) AddDataShard(clid uint, shkey kr.ShardKey, tsa tsa.TSA) error {
	v := srv.shard.Load()
	if v != nil {
		return fmt.Errorf("single datashard " +
			"server does not support more than 1 datashard connection simultaneously")
	}

	if val, err := srv.pool.ConnectionWithTSA(clid, shkey, tsa); err != nil {
		return err
	} else {
		srv.shard.Store(&val)
	}

	return nil
}

// TODO : unit tests
func (srv *ShardServer) Send(query pgproto3.FrontendMessage) error {
	v := srv.shard.Load()

	if v == nil {
		return ErrShardUnavailable
	}
	spqrlog.Zero.Debug().
		Uint("single-shard", (*v).ID()).
		Type("query-type", query).
		Msg("single-shard sending msg to server")

	return (*v).Send(query)
}

// TODO : unit tests
func (srv *ShardServer) SendShard(query pgproto3.FrontendMessage, shkey kr.ShardKey) error {
	if (*srv.shard.Load()).SHKey().Name != shkey.Name {
		return spqrerror.NewByCode(spqrerror.SPQR_NO_DATASHARD)
	}
	return srv.Send(query)
}

// TODO : unit tests
func (srv *ShardServer) Receive() (pgproto3.BackendMessage, uint, error) {

	msg, err := (*srv.shard.Load()).Receive()
	spqrlog.Zero.Debug().
		Uint("single-shard", (*srv.shard.Load()).ID()).
		Type("message-type", msg).
		Str("txstatus", srv.TxStatus().String()).
		Uints("shards", shard.ShardIDs(srv.Datashards())).
		Err(err).
		Msg("single-shard receiving msg from server")

	return msg, 0, err
}

// TODO : unit tests
func (srv *ShardServer) ReceiveShard(shardId uint) (pgproto3.BackendMessage, error) {
	if (*srv.shard.Load()).ID() != shardId {
		return nil, spqrerror.NewByCode(spqrerror.SPQR_NO_DATASHARD)
	}
	msg, _, err := srv.Receive()
	return msg, err
}

// TODO : unit tests
func (srv *ShardServer) Cleanup(rule *config.FrontendRule) error {
	return srv.cleanupLockFree(*srv.shard.Load(), rule)
}

// TODO : unit tests
func (srv *ShardServer) cleanupLockFree(v shard.ShardHostInstance, rule *config.FrontendRule) error {
	if v == nil {
		return ErrShardUnavailable
	}
	return v.Cleanup(rule)
}

// TODO : unit tests
func (srv *ShardServer) Cancel() error {
	v := srv.shard.Load()
	if v == nil {
		return ErrShardUnavailable
	}
	return (*v).Cancel()
}

// TODO : unit tests
func (srv *ShardServer) SetTxStatus(tx txstatus.TXStatus) {
	v := srv.shard.Load()
	if v != nil {
		(*v).SetTxStatus(tx)
	}
}

// TODO : unit tests
func (srv *ShardServer) TxStatus() txstatus.TXStatus {
	v := srv.shard.Load()
	if v == nil {
		return txstatus.TXERR
	}
	return (*v).TxStatus()
}

// TODO : unit tests
func (srv *ShardServer) Datashards() []shard.ShardHostInstance {
	var rv []shard.ShardHostInstance = nil
	v := srv.shard.Load()

	if v != nil {
		rv = []shard.ShardHostInstance{*v}
	}

	return rv
}

var _ Server = &ShardServer{}
