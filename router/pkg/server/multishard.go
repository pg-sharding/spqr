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
	ShardCCState
	ShardCopyState
	ShardRFQState
	ErrorState
)

type MultishardState int

const (
	InitialState = MultishardState(iota)
	RunningState
	ServerErrorState
	CommandCompleteState
	CopyState
)

type MultiShardServer struct {
	rule         *config.BackendRule
	activeShards []datashard.Shard

	states []ShardState

	multistate MultishardState

	pool datashard.DBPool

	copyBuf []*pgproto3.CopyOutResponse
}

func (m *MultiShardServer) HasPrepareStatement(hash uint64) bool {
	panic("implement me")
}

func (m *MultiShardServer) PrepareStatement(hash uint64) {}

func (m *MultiShardServer) Reset() error {
	return nil
}

func (m *MultiShardServer) AddDataShard(shkey kr.ShardKey, tsa string) error {
	sh, err := m.pool.Connection(shkey, m.rule, tsa)
	if err != nil {
		return err
	}

	m.activeShards = append(m.activeShards, sh)
	m.states = append(m.states, ShardRFQState)
	m.multistate = InitialState

	return nil
}

func (m *MultiShardServer) UnRouteShard(sh kr.ShardKey, rule *config.FrontendRule) error {
	for _, activeShard := range m.activeShards {
		if activeShard.Name() == sh.Name {
			return activeShard.Cleanup(rule)
		}
	}

	return fmt.Errorf("unrouted datashard does not match any of active")
}

func (m *MultiShardServer) Name() string {
	return "multishard"
}

func (m *MultiShardServer) AddTLSConf(cfg *tls.Config) error {
	for _, shard := range m.activeShards {
		_ = shard.AddTLSConf(cfg)
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
			if m.states[i] == ShardRFQState {
				continue
			}
			// error state or something else
			m.states[i] = ShardRFQState

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
						spqrlog.Logger.Printf(spqrlog.LOG, "multishard server: got %T message from %s shard while rollback after error", msg, m.activeShards[i].Name())
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
		m.copyBuf = nil
		var saveRd *pgproto3.RowDescription = nil
		var saveCC *pgproto3.CommandComplete = nil
		var saveRFQ *pgproto3.ReadyForQuery = nil
		/* Step one: ensure all shard backend are stared */
		for i := range m.activeShards {
			for {
				// all shards should be in rfq state
				msg, err := m.activeShards[i].Receive()
				if err != nil {
					spqrlog.Logger.Printf(spqrlog.LOG, "multishard server: encountered error while reading from %s shard", m.activeShards[i].Name())
					m.states[i] = ErrorState
					rollback()
					return nil, err
				}

				spqrlog.Logger.Printf(spqrlog.DEBUG2, "multishard server init: got %v msg from %s shard", msg, m.activeShards[i].Name())

				switch retMsg := msg.(type) {
				case *pgproto3.CopyOutResponse:
					if m.multistate != InitialState && m.multistate != CopyState {
						return nil, MultiShardSyncBroken
					}
					m.states[i] = ShardCopyState
					m.multistate = CopyState
					m.copyBuf = append(m.copyBuf, retMsg)
				case *pgproto3.CommandComplete:
					m.states[i] = ShardCCState
					saveCC = retMsg //
				case *pgproto3.RowDescription:
					m.states[i] = DatarowState
					saveRd = retMsg // all should be same
				case *pgproto3.ReadyForQuery:
					if m.multistate != InitialState {
						return nil, MultiShardSyncBroken
					}
					m.states[i] = ShardRFQState
					saveRFQ = retMsg
				case *pgproto3.ParameterStatus:
					m.states[i] = DatarowState
					// ignore
					// XXX: do not ignore
					continue
				case *pgproto3.NoticeResponse:
					// thats ok
					continue
				case *pgproto3.ErrorResponse:
					if m.multistate != InitialState {
						return nil, MultiShardSyncBroken
					}
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
		if m.multistate == CopyState {
			n := len(m.copyBuf)
			var currMsg *pgproto3.CopyOutResponse
			m.copyBuf, currMsg = m.copyBuf[n-2:], m.copyBuf[n-1]

			spqrlog.Logger.Printf(spqrlog.DEBUG5, "miltishard server: flush copy buff, new len %d", len(m.copyBuf))
			return currMsg, nil
		}

		m.multistate = RunningState
		return saveRd, nil
	case CopyState:
		if len(m.copyBuf) > 0 {
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "miltishard server: flush copy buff")
			n := len(m.copyBuf)
			var currMsg *pgproto3.CopyOutResponse
			m.copyBuf, currMsg = m.copyBuf[n-2:], m.copyBuf[n-1]
			return currMsg, nil
		}
		/* Step two: fetch all copy out resp */
		for i := range m.activeShards {
			// some shards may be in cc state, some in copy state

			if m.states[i] == ShardCCState {
				continue
			}
			if m.states[i] != ShardCopyState {
				return nil, MultiShardSyncBroken
			}

			msg, err := m.activeShards[i].Receive()
			if err != nil {
				spqrlog.Logger.Printf(spqrlog.LOG, "multishard server: encountered error while reading from %s shard", m.activeShards[i].Name())
				m.states[i] = ErrorState
				rollback()
				return nil, err
			}

			spqrlog.Logger.Printf(spqrlog.LOG, "multishard server: got %T from %s shard", msg, m.activeShards[i].Name())

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
	case RunningState:
		/* Step two: fetch all datarow ms	gs */
		for i := range m.activeShards {
			// some shards may be in cc state

			if m.states[i] == ShardCCState {
				continue
			}

			msg, err := m.activeShards[i].Receive()
			if err != nil {
				spqrlog.Logger.Printf(spqrlog.LOG, "multishard server: encountered error while reading from %s shard", m.activeShards[i].Name())
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
		spqrlog.Logger.Printf(spqrlog.LOG, "multishard server: enter rfq await mode")

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
						m.states[i] = ShardRFQState
						return nil
					default:
						// sync is broken
						return MultiShardSyncBroken
					}
				}
			}(); err != nil {
				spqrlog.Logger.Printf(spqrlog.LOG, "multishard server: encountered error %v while reading from %s shard", err, m.activeShards[i].Name())
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

func (m *MultiShardServer) Cleanup(rule config.FrontendRule) error {

	if rule.PoolRollback {
		if err := m.Send(&pgproto3.Query{
			String: "ROLLBACK",
		}); err != nil {
			return err
		}
	}

	if rule.PoolDiscard {
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

func (m *MultiShardServer) Cancel() error {
	var err error
	for _, sh := range m.activeShards {
		err = sh.Cancel()
	}
	return err
}

var _ Server = &MultiShardServer{}
