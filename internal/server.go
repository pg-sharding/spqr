package internal

import (
	"crypto/tls"
	"sync"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type Server interface {
	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	AddShard(shkey ShardKey) error
	UnrouteShard(sh ShardKey) error

	AddTLSConf(cfg *tls.Config) error

	Cleanup() error
}

type ShardServer struct {
	rule *config.BERule

	pool ShardPool

	shard Shard
}

func (srv *ShardServer) UnrouteShard(shkey ShardKey) error {

	if srv.shard.SHKey() != shkey {
		return xerrors.New("active shard does not match unrouted")
	}

	if err := srv.pool.Put(srv.shard); err != nil {
		return err
	}

	srv.shard = nil

	return nil
}

func (srv *ShardServer) AddShard(shkey ShardKey) error {
	if srv.shard != nil {
		return xerrors.New("single shard server does not support more than 2 shard connection simultaneously")
	}

	if sh, err := srv.pool.Connection(shkey); err != nil {
		return err
	} else {
		srv.shard = sh
	}

	return nil
}

func (srv *ShardServer) AddTLSConf(cfg *tls.Config) error {
	return srv.shard.ReqBackendSsl(cfg)
}

func NewShardServer(rule *config.BERule, spool ShardPool) *ShardServer {
	return &ShardServer{
		rule: rule,
		pool: spool,
	}
}

func (srv *ShardServer) Send(query pgproto3.FrontendMessage) error {
	return srv.shard.Send(query)
}

func (srv *ShardServer) Receive() (pgproto3.BackendMessage, error) {
	return srv.shard.Receive()
}

func (srv *ShardServer) Cleanup() error {

	if srv.rule.PoolRollback {
		if err := srv.Send(&pgproto3.Query{
			String: "ROLLBACK",
		}); err != nil {
			return err
		}
	}

	if srv.rule.PoolDiscard {
		if err := srv.Send(&pgproto3.Query{
			String: "DISCARD ALL",
		}); err != nil {
			return err
		}
	}

	return nil
}

var _ Server = &ShardServer{}

type MultiShardServer struct {
	rule       *config.BERule
	activePool ShardPool

	pool ShardPool
}

func (m *MultiShardServer) AddShard(shkey ShardKey) error {
	sh, err := m.pool.Connection(shkey)
	if err != nil {
		return err
	}

	return m.activePool.Put(sh)
}

func (m *MultiShardServer) UnrouteShard(sh ShardKey) error {

	if !m.activePool.Check(sh) {
		return xerrors.New("unrouted shard does not match any of active")
	}

	return nil
}

func (m *MultiShardServer) AddTLSConf(cfg *tls.Config) error {
	for _, shard := range m.activePool.List() {
		_ = shard.ReqBackendSsl(cfg)
	}

	return nil
}

func (m *MultiShardServer) Send(msg pgproto3.FrontendMessage) error {
	for _, shard := range m.activePool.List() {
		tracelog.InfoLogger.Printf("sending Q to sh %v", shard.Name())
		err := shard.Send(msg)
		if err != nil {
			tracelog.InfoLogger.PrintError(err)
			//
		}
	}

	return nil
}

func (m *MultiShardServer) Receive() (pgproto3.BackendMessage, error) {

	ret := &pgproto3.DataRow{
		Values: [][]byte{},
	}

	mu := sync.Mutex{}

	for _, shard := range m.activePool.List() {
		tracelog.InfoLogger.Printf("recv mult resp from sh %s", shard.Name())

		go func(shard Shard) error {

			msg, err := shard.Receive()
			if err != nil {
				return err
			}
			tracelog.InfoLogger.Printf("got %v from %s", msg, shard.Name())

			switch v := msg.(type) {
			case *pgproto3.DataRow:
				mu.Lock()
				ret.Values = append(ret.Values, v.Values...)
				mu.Unlock()
			case *pgproto3.ReadyForQuery:
				return nil
			default:
				tracelog.ErrorLogger.Printf("unexcepted msg from server %T", v)
				return xerrors.New("unexcepted msg from server")
			}
			return nil
		}(shard)
	}

	if len(ret.Values) == 0 {
		return &pgproto3.ReadyForQuery{}, nil
	}

	return ret, nil
}

func (m *MultiShardServer) Cleanup() error {

	if m.rule.PoolRollback {
		if err := m.Send(&pgproto3.Query{
			String: "ROLLBACK",
		}); err != nil {
			return err
		}
	}

	if m.rule.PoolDiscard {
		if err := m.Send(&pgproto3.Query{
			String: "DISCARD ALL",
		}); err != nil {
			return err
		}
	}

	return nil
}

var _ Server = &MultiShardServer{}

func NewMultiShardServer(rule *config.BERule, pool ShardPool) (Server, error) {
	ret := &MultiShardServer{
		rule:       rule,
		pool:       pool,
		activePool: NewShardPool(map[string]*config.ShardCfg{}),
	}

	return ret, nil
}
