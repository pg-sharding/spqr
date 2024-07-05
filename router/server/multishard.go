package server

import (
	"crypto/tls"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
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
	activeShards []shard.Shard

	states []ShardState

	multistate MultishardState

	pool pool.DBPool

	status txstatus.TXStatus

	copyBuf []*pgproto3.CopyOutResponse
}

// RequestData implements Server.
func (m *MultiShardServer) RequestData() {
	panic("unimplemented")
}

// DataPending implements Server.
func (m *MultiShardServer) DataPending() bool {
	panic("unimplemented")
}

func (m *MultiShardServer) HasPrepareStatement(hash uint64) (bool, *shard.PreparedStatementDescriptor) {
	panic("implement me")
}

func (m *MultiShardServer) PrepareStatement(hash uint64, rd *shard.PreparedStatementDescriptor) {}

func (m *MultiShardServer) Reset() error {
	return nil
}

func (m *MultiShardServer) AddDataShard(clid uint, shkey kr.ShardKey, tsa string) error {
	sh, err := m.pool.Connection(clid, shkey, tsa)
	if err != nil {
		return err
	}

	m.activeShards = append(m.activeShards, sh)
	m.states = append(m.states, ShardRFQState)
	m.multistate = InitialState

	return nil
}

func (m *MultiShardServer) UnRouteShard(sh kr.ShardKey, rule *config.FrontendRule) error {
	// map?
	for _, activeShard := range m.activeShards {
		if activeShard.Name() == sh.Name {
			err := activeShard.Cleanup(rule)

			if err := m.pool.Put(activeShard); err != nil {
				return err
			}

			return err
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
		spqrlog.Zero.Debug().
			Uint("shard", shard.ID()).
			Interface("message", msg).
			Msg("sending message to shard")
		if err := shard.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
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
			spqrlog.Zero.Debug().
				Uint("shard", m.activeShards[i].ID()).
				Msg("rollback shard in multishard after error")
			if m.activeShards[i].Sync() == 0 {
				continue
			}
			// error state or something else
			m.states[i] = ShardRFQState

			go func(i int) {
				for {
					msg, err := m.activeShards[i].Receive()
					if err != nil {
						spqrlog.Zero.Error().Err(err).Msg("")
						return
					}

					switch msg.(type) {
					case *pgproto3.ReadyForQuery:
						return
					default:
						spqrlog.Zero.Info().
							Uint("shard", m.activeShards[i].ID()).
							Type("message-type", msg).
							Msg("multishard server: recived message from shard while rollback after error")
					}
				}
			}(i)
		}
	}

	switch m.multistate {
	case ServerErrorState:
		m.multistate = InitialState
		return &pgproto3.ReadyForQuery{
			TxStatus: byte(txstatus.TXIDLE), // XXX : fix this
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
					spqrlog.Zero.Info().
						Uint("shard", m.activeShards[i].ID()).
						Err(err).
						Msg("multishard server: encountered error while reading from shard")
					m.states[i] = ErrorState
					rollback()
					return nil, err
				}

				spqrlog.Zero.Debug().
					Interface("message", msg).
					Uint("shard", m.activeShards[i].ID()).
					Msg("multishard server init: recieved message from shard")

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
					spqrlog.Zero.Error().
						Uint("client", spqrlog.GetPointer(m)).
						Str("message", retMsg.Message).
						Msg("multishard server received error")
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

			spqrlog.Zero.Debug().
				Int("new-buff-len", len(m.copyBuf)).
				Msg("miltishard server: flush copy buff")
			return currMsg, nil
		}

		m.multistate = RunningState
		return saveRd, nil
	case CopyState:
		if len(m.copyBuf) > 0 {
			spqrlog.Zero.Debug().Msg("miltishard server: flush copy buff")
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
				spqrlog.Zero.Info().
					Uint("shard", m.activeShards[i].ID()).
					Err(err).
					Msg("multishard server: encountered error while reading from shard")
				m.states[i] = ErrorState
				rollback()
				return nil, err
			}
			spqrlog.Zero.Info().
				Uint("shard", m.activeShards[i].ID()).
				Type("message-type", msg).
				Msg("multishard server: recived message from shard")

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
				spqrlog.Zero.Info().
					Uint("shard", m.activeShards[i].ID()).
					Err(err).
					Msg("multishard server: encountered error while reading from shard")
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
		spqrlog.Zero.Info().Msg("multishard server: enter rfq await mode")

		/* Step tree: fetch all datarow msgs */
		for i := range m.activeShards {
			// all shards shall be in cc state
			spqrlog.Zero.Info().Uint("shard", m.activeShards[i].ID()).Msg("multishard server: await server")

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
				spqrlog.Zero.Info().
					Uint("shard", m.activeShards[i].ID()).
					Err(err).
					Msg("multishard server: encountered error while reading from shard")
				m.states[i] = ErrorState
				rollback()
				return nil, err
			}
		}

		m.multistate = InitialState
		m.status = txstatus.TXIDLE
		return &pgproto3.ReadyForQuery{
			TxStatus: byte(txstatus.TXIDLE), // XXX : fix this
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

func (m *MultiShardServer) Sync() int64 {
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

func (m *MultiShardServer) SetTxStatus(tx txstatus.TXStatus) {
	m.status = tx
}

func (m *MultiShardServer) TxStatus() txstatus.TXStatus {
	return txstatus.TXIDLE
}

func (m *MultiShardServer) Datashards() []shard.Shard {
	return m.activeShards
}

var _ Server = &MultiShardServer{}
