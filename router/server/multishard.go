package server

import (
	"fmt"

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
	CopyOutState
	CopyInState
)

type multishardPStmtKey struct {
	ShardId uint
	Hash    uint64
}

type MultiShardServer struct {
	activeShards []shard.Shard

	states []ShardState

	multistate MultishardState

	pool *pool.DBPool

	status txstatus.TXStatus

	copyBuf []*pgproto3.CopyOutResponse

	stmtDefByShard  map[multishardPStmtKey]*prepstatement.PreparedStatementDefinition
	stmtDescByShard map[multishardPStmtKey]*prepstatement.PreparedStatementDescriptor
}

func NewMultiShardServer(pool *pool.DBPool) (Server, error) {
	ret := &MultiShardServer{
		pool:            pool,
		activeShards:    []shard.Shard{},
		stmtDefByShard:  make(map[multishardPStmtKey]*prepstatement.PreparedStatementDefinition),
		stmtDescByShard: make(map[multishardPStmtKey]*prepstatement.PreparedStatementDescriptor),
	}

	return ret, nil
}

// HasPrepareStatement implements Server.
func (m *MultiShardServer) HasPrepareStatement(hash uint64, shardId uint) (bool, *prepstatement.PreparedStatementDescriptor) {
	desc, ok := m.stmtDescByShard[multishardPStmtKey{ShardId: shardId, Hash: hash}]
	return ok, desc
}

// StorePrepareStatement implements Server.
func (m *MultiShardServer) StorePrepareStatement(hash uint64, shardId uint, d *prepstatement.PreparedStatementDefinition, rd *prepstatement.PreparedStatementDescriptor) error {
	key := multishardPStmtKey{Hash: hash, ShardId: shardId}
	m.stmtDescByShard[key] = rd
	m.stmtDefByShard[key] = d
	return nil
}

// DataPending implements Server.
func (m *MultiShardServer) DataPending() bool {
	for _, shard := range m.activeShards {
		if shard.DataPending() {
			return true
		}
	}
	return false
}

func (m *MultiShardServer) Reset() error {
	return nil
}

func (m *MultiShardServer) AddDataShard(clid uint, shkey kr.ShardKey, tsa tsa.TSA) error {
	for _, piv := range m.activeShards {
		if piv.SHKey().Name == shkey.Name {
			return fmt.Errorf("multishard connection already use %v", shkey.Name)
		}
	}
	sh, err := m.pool.ConnectionWithTSA(clid, shkey, tsa)
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

func (m *MultiShardServer) SendShard(msg pgproto3.FrontendMessage, shardId uint) error {
	anyShard := false
	for _, shard := range m.activeShards {
		if shard.ID() != shardId {
			continue
		}
		anyShard = true
		spqrlog.Zero.Debug().
			Uint("shard", shard.ID()).
			Interface("message", msg).
			Msg("sending message to shard")
		if err := shard.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	if !anyShard {
		return spqrerror.Newf(spqrerror.SPQR_NO_DATASHARD, "attempt to send message to nonexistent datashard \"%d\"", shardId)
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
		var saveCIn *pgproto3.CopyInResponse = nil
		/* Step one: ensure all shard backend are started */
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
					Msg("multishard server init: received message from shard")

				switch retMsg := msg.(type) {
				case *pgproto3.ParseComplete:
					// that's ok
					continue
				case *pgproto3.BindComplete:
					// that's also ok
					continue
				case *pgproto3.CopyOutResponse:
					if m.multistate != InitialState && m.multistate != CopyOutState {
						return nil, MultiShardSyncBroken
					}
					m.states[i] = ShardCopyState
					m.multistate = CopyOutState
					m.copyBuf = append(m.copyBuf, retMsg)
				case *pgproto3.CopyInResponse:
					if m.multistate != InitialState && m.multistate != CopyInState {
						return nil, MultiShardSyncBroken
					}
					m.states[i] = ShardCopyState
					m.multistate = CopyInState
					saveCIn = retMsg
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
		if m.multistate == CopyOutState {
			n := len(m.copyBuf)
			var currMsg *pgproto3.CopyOutResponse
			m.copyBuf, currMsg = m.copyBuf[n-2:], m.copyBuf[n-1]

			spqrlog.Zero.Debug().
				Int("new-buff-len", len(m.copyBuf)).
				Msg("miltishard server: flush copy buff")
			return currMsg, nil
		}
		if m.multistate == CopyInState {
			m.multistate = RunningState
			return saveCIn, nil
		}

		m.multistate = RunningState
		return saveRd, nil
	case CopyOutState:
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
				Msg("multishard server: received message from shard")

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
	case CopyInState:
		return &pgproto3.CommandComplete{
			CommandTag: []byte{},
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
	var syncCount int64
	for _, shard := range m.activeShards {
		syncCount += shard.Sync()
	}
	return syncCount
}

func (m *MultiShardServer) Cancel() error {
	var errs []error
	for _, sh := range m.activeShards {
		if err := sh.Cancel(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors occurred during cancel: %w", fmt.Errorf("%v", errs))
	}
	return nil
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
