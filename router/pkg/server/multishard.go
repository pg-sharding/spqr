package server

import (
	"crypto/tls"
	"fmt"
	"reflect"
	"sync"

	"github.com/jackc/pgproto3/v2"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/pkg/datashard"
)

type MultiShardServer struct {
	rule         *config.BERule
	activeShards []datashard.Shard

	pool datashard.DBPool
}

func (m *MultiShardServer) HasPrepareStatement(hash uint64) bool {
	panic("implement me")
}

func (m *MultiShardServer) PrepareStatement(hash uint64) {}

func (m *MultiShardServer) Reset() error {
	return nil
}

func (m *MultiShardServer) AddShard(shkey kr.ShardKey) error {
	sh, err := m.pool.Connection(shkey, m.rule)
	if err != nil {
		return err
	}

	m.activeShards = append(m.activeShards, sh)
	return nil
}

func (m *MultiShardServer) UnRouteShard(sh kr.ShardKey) error {

	for _, activeShard := range m.activeShards {
		if activeShard.Name() == sh.Name {
			return nil
		}
	}

	return xerrors.New("unrouted datashard does not match any of active")
}

func (m *MultiShardServer) AddTLSConf(cfg *tls.Config) error {
	for _, shard := range m.activeShards {
		_ = shard.ReqBackendSsl(cfg)
	}

	return nil
}

func (m *MultiShardServer) Send(msg pgproto3.FrontendMessage) error {
	for _, shard := range m.activeShards {
		spqrlog.Logger.Printf(spqrlog.DEBUG4, "sending %+v to sh %v", msg, shard.Name())
		err := shard.Send(msg)
		if err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return nil
}

func (m *MultiShardServer) Receive() (pgproto3.BackendMessage, error) {
	ch := make(chan pgproto3.BackendMessage, len(m.activeShards))
	errch := make(chan error, 0)
	defer close(errch)

	wg := sync.WaitGroup{}

	wg.Add(len(m.activeShards))
	for _, currshard := range m.activeShards {
		spqrlog.Logger.Printf(spqrlog.DEBUG2, "recv mult resp from sh %s", currshard.Name())

		currshard := currshard
		go func() {
			err := func(shard datashard.Shard, ch chan<- pgproto3.BackendMessage, wg *sync.WaitGroup) error {
				defer wg.Done()

				msg, err := shard.Receive()
				if err != nil {
					return err
				}
				spqrlog.Logger.Printf(spqrlog.DEBUG2, "got %v from %s", msg, shard.Name())

				ch <- msg

				return nil
			}(currshard, ch, &wg)

			if err != nil {
				errch <- err
			}
		}()
	}

	wg.Wait()
	close(ch)

	msgs := make([]pgproto3.BackendMessage, 0, len(m.activeShards))

	err := func() error {
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					// all shards messages are collected
					return nil
				}
				msgs = append(msgs, msg)
			case err := <-errch:
				return err
			}
		}
	}()

	if err != nil {
		return nil, err
	}

	for i := range msgs {
		if reflect.TypeOf(msgs[0]) != reflect.TypeOf(msgs[i]) {
			return nil, fmt.Errorf("got messages with different types from multiconnection %T, %T", msgs[0], msgs[i])
		}
	}

	tracelog.InfoLogger.Printf("compute multi server msgs from %T", msgs[0])

	switch v := msgs[0].(type) {
	case *pgproto3.CommandComplete:
		return v, nil
	case *pgproto3.ErrorResponse:
		return v, nil
	case *pgproto3.ReadyForQuery:
		return v, nil
	case *pgproto3.DataRow:
		ret := &pgproto3.DataRow{}

		for i, msg := range msgs {
			if i == 0 {
				ret = msg.(*pgproto3.DataRow)
				continue
			}
			drow := msg.(*pgproto3.DataRow)
			ret.Values = append(ret.Values, drow.Values...)
		}

		return ret, nil

	default:
		return &pgproto3.ErrorResponse{Severity: "ERROR", Message: fmt.Sprintf("failed to conpose responce %T", v)}, nil
	}
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

func (m *MultiShardServer) Sync() int {
	//TODO implement me
	panic("implement me")
}

var _ Server = &MultiShardServer{}
