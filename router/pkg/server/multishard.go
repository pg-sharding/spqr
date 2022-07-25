package server

import (
	"crypto/tls"
	"fmt"
	"github.com/jackc/pgproto3/v2"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/pkg/datashard"
)

type ShardState int

const (
	DatarowState = ShardState(iota)
	ShardCCState = ShardState(iota)
	RFQState
	ErrorState
)

type MultishardState int

const (
	InitialState = MultishardState(iota)
	RunningState
	ServerErrorState
	CommandCompleteState
)

type MultiShardServer struct {
	rule         *config.BERule
	activeShards []datashard.Shard

	states []ShardState

	multistate MultishardState

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
	m.states = append(m.states, RFQState)
	m.multistate = InitialState

	return nil
}

func (m *MultiShardServer) UnRouteShard(sh kr.ShardKey) error {
	for _, activeShard := range m.activeShards {
		if activeShard.Name() == sh.Name {
			return nil
		}
	}

	return fmt.Errorf("unrouted datashard does not match any of active")
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

var (
	MultiShardSyncBroken = fmt.Errorf("multishard state is out of sync")
)

func (m *MultiShardServer) Receive() (pgproto3.BackendMessage, error) {

	rollback := func() {
		for i := range m.activeShards {
			if m.states[i] == RFQState {
				continue
			}
			// error state or something else
			m.states[i] = RFQState

			go func(i int) {
				for {
					msg, err := m.activeShards[i].Receive()
					if err != nil {
						spqrlog.Logger.PrintError(err)
						return
					}

					switch msg.(type) {
					case *pgproto3.ReadyForQuery:
						return
					default:
						spqrlog.Logger.Printf(spqrlog.LOG, "got %T message from %s shard while rollback after error", msg, m.activeShards[i].Name())
					}
				}
			}(i)
		}
	}

	switch m.multistate {
	case ServerErrorState:
		m.multistate = InitialState
		return &pgproto3.ReadyForQuery{
			TxStatus: 0, // XXX : fix this
		}, nil
	case InitialState:
		var saveRd *pgproto3.RowDescription = nil
		var saveCC *pgproto3.CommandComplete = nil
		var saveRFQ *pgproto3.ReadyForQuery = nil
		/* Step one: ensure all shard backend are stared */
		for i := range m.activeShards {

			for {
				// all shards should be in rfq state
				msg, err := m.activeShards[i].Receive()
				if err != nil {
					spqrlog.Logger.Printf(spqrlog.LOG, "encountered error while reading from %s shard", m.activeShards[i].Name())
					m.states[i] = ErrorState
					rollback()
					return nil, err
				}

				spqrlog.Logger.Printf(spqrlog.DEBUG2, "got %T msg from %s shard", msg, m.activeShards[i].Name())

				switch retMsg := msg.(type) {
				case *pgproto3.CommandComplete:
					saveCC = retMsg //
				case *pgproto3.RowDescription:
					saveRd = retMsg // all should be same
				case *pgproto3.ReadyForQuery:
					saveRFQ = retMsg
				case *pgproto3.ParameterStatus:
					// ignore
					// XXX: do not ignore
					continue
				case *pgproto3.ErrorResponse:
					spqrlog.Logger.Errorf("err got is %v", retMsg.Message)
					m.states[i] = ErrorState
					m.multistate = ServerErrorState
					rollback()
					return msg, nil
				default:
					m.states[i] = ErrorState
					rollback()
					// sync is broken
					return nil, MultiShardSyncBroken
				}
				m.states[i] = DatarowState
				break
			}
		}

		if saveCC != nil {
			m.multistate = InitialState
			return saveCC, nil
		}
		if saveRFQ != nil {
			m.multistate = InitialState
			return saveRFQ, nil
		}

		m.multistate = RunningState
		return saveRd, nil
	case RunningState:
		/* Step two: fetch all datarow ms	gs */
		for i := range m.activeShards {
			// some shards may be in cc state

			if m.states[i] == ShardCCState {
				continue
			}

			msg, err := m.activeShards[i].Receive()
			if err != nil {
				spqrlog.Logger.Printf(spqrlog.LOG, "encountered error while reading from %s shard", m.activeShards[i].Name())
				m.states[i] = ErrorState
				rollback()
				return nil, err
			}

			switch msg.(type) {
			case *pgproto3.CommandComplete:
				//
				m.states[i] = ShardCCState
			case *pgproto3.ReadyForQuery:
				m.states[i] = ErrorState
				rollback()
				// sync is broken
				return nil, MultiShardSyncBroken
			default:
				return msg, nil
			}
		}
		// all shard are in RFQ state
		m.multistate = CommandCompleteState
		return &pgproto3.CommandComplete{
			CommandTag: []byte{}, // XXX : fix this
		}, nil
	case CommandCompleteState:
		spqrlog.Logger.Printf(spqrlog.LOG, "enter rfq await mode")

		/* Step tree: fetch all datarow msgs */
		for i := range m.activeShards {
			// all shards shall be in cc state

			if m.states[i] != ShardCCState {
				return nil, MultiShardSyncBroken
			}

			if err := func() error {
				for {
					msg, err := m.activeShards[i].Receive()
					if err != nil {
						return err
					}

					switch msg.(type) {
					case *pgproto3.ReadyForQuery:
						m.states[i] = RFQState
						return nil
					default:
						// sync is broken
						return MultiShardSyncBroken
					}
				}
			}(); err != nil {
				spqrlog.Logger.Printf(spqrlog.LOG, "encountered error %v while reading from %s shard", err, m.activeShards[i].Name())
				m.states[i] = ErrorState
				rollback()
				return nil, err
			}
		}

		m.multistate = InitialState
		return &pgproto3.ReadyForQuery{
			TxStatus: 0, // XXX : fix this
		}, nil
	}

	return nil, nil
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
