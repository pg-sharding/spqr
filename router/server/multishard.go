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
	CopyInState
)

type MultiShardServer struct {
	activeShards []shard.ShardHostInstance

	states []ShardState

	multistate MultishardState

	pool pool.MultiShardTSAPool

	status txstatus.TXStatus

	copyBuf []*pgproto3.CopyOutResponse
}

// ToMultishard implements Server.
func (m *MultiShardServer) ToMultishard() Server {
	return m
}

func NewMultiShardServer(pool pool.MultiShardTSAPool) (Server, error) {
	ret := &MultiShardServer{
		pool:         pool,
		activeShards: []shard.ShardHostInstance{},
	}

	return ret, nil
}

func NewMultiShardServerFromShard(pool pool.MultiShardTSAPool, sh shard.ShardHostInstance) Server {
	return &MultiShardServer{
		pool: pool,
		activeShards: []shard.ShardHostInstance{
			sh,
		},
		multistate: InitialState,
		states:     []ShardState{ShardRFQState},
		status:     sh.TxStatus(),
	}
}

// HasPrepareStatement implements Server.
func (m *MultiShardServer) HasPrepareStatement(hash uint64, shardId uint) (bool, *prepstatement.PreparedStatementDescriptor) {
	for _, shard := range m.activeShards {
		if shard.ID() == shardId {
			return shard.HasPrepareStatement(hash, shardId)
		}
	}
	return false, nil
}

// StorePrepareStatement implements Server.
func (m *MultiShardServer) StorePrepareStatement(hash uint64, shardId uint, d *prepstatement.PreparedStatementDefinition, rd *prepstatement.PreparedStatementDescriptor) error {
	for _, shard := range m.activeShards {
		if shard.ID() == shardId {
			return shard.StorePrepareStatement(hash, shardId, d, rd)
		}
	}
	return spqrerror.Newf(spqrerror.SPQR_NO_DATASHARD, "shard \"%d\" not found", shardId)
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

func (m *MultiShardServer) expandGangUtil(clid uint, shkey kr.ShardKey, tsa tsa.TSA, deployTX bool) error {
	for _, piv := range m.activeShards {
		if piv.SHKey().Name == shkey.Name {

			spqrlog.Zero.Debug().Uint("shid", piv.ID()).Msg("reuse  gang member")

			/* todo: multi-slice server can use multiple connections to shard. */
			return nil
		}
	}
	sh, err := m.pool.ConnectionWithTSA(clid, shkey, tsa)
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().Uint("shid", sh.ID()).Msg("acquired gang member")

	if deployTX {
		retst, err := shard.DeployTxOnShard(sh, &pgproto3.Query{
			String: "BEGIN",
		}, txstatus.TXACT)

		if err != nil {
			return err
		} else if retst != txstatus.TXACT {
			return fmt.Errorf("failed to expand transaction on shard")
		}
	}

	m.activeShards = append(m.activeShards, sh)
	m.states = append(m.states, m.states[0])

	return nil
}

func (m *MultiShardServer) AllocateGangMember(clid uint, shkey kr.ShardKey, tsa tsa.TSA) error {
	if err := m.expandGangUtil(clid, shkey, tsa, false); err != nil {
		return err
	}
	m.multistate = InitialState

	return nil
}

func (m *MultiShardServer) ExpandGang(clid uint, shkey kr.ShardKey, tsa tsa.TSA, deployTX bool) error {
	return m.expandGangUtil(clid, shkey, tsa, deployTX)
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

func (m *MultiShardServer) SendShard(msg pgproto3.FrontendMessage, shkey kr.ShardKey) error {
	anyShard := false
	for _, shard := range m.activeShards {
		if shard.SHKey().Name != shkey.Name {
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
		return spqrerror.Newf(spqrerror.SPQR_NO_DATASHARD, "attempt to send message to nonexistent datashard \"%s\"", shkey.Name)
	}
	return nil
}

var ErrMultiShardSyncBroken = fmt.Errorf("multishard state is out of sync")

func (m *MultiShardServer) Receive() (pgproto3.BackendMessage, uint, error) {
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
							Msg("multishard server: received message from shard while rollback after error")
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
		}, 0, nil
	case InitialState:
		m.copyBuf = nil
		var saveRd *pgproto3.RowDescription = nil
		var saveCC *pgproto3.CommandComplete = nil
		var saveRFQ *pgproto3.ReadyForQuery = nil
		var saveCIn *pgproto3.CopyInResponse = nil
		/* Step one: ensure all shard backend are started */
		for i := range m.activeShards {
			/* maybe query ass partially dispatched */
			if m.activeShards[i].Sync() == 0 {
				continue
			}
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
					return nil, uint(i), err
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
					return nil, 0, ErrMultiShardSyncBroken
				case *pgproto3.CopyInResponse:
					if m.multistate != InitialState && m.multistate != CopyInState {
						return nil, 0, ErrMultiShardSyncBroken
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
						return nil, 0, ErrMultiShardSyncBroken
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
						return nil, 0, ErrMultiShardSyncBroken
					}
					spqrlog.Zero.Error().
						Uint("client", spqrlog.GetPointer(m)).
						Str("message", retMsg.Message).
						Msg("multishard server received error")
					m.states[i] = ErrorState
					m.multistate = ServerErrorState
					rollback()
					return msg, uint(i), nil
				default:

					m.states[i] = ErrorState
					rollback()
					// sync is broken
					return nil, 0, ErrMultiShardSyncBroken
				}
				break
			}
		}

		if saveCC != nil {
			m.multistate = InitialState
			return saveCC, 0, nil
		}
		if saveRFQ != nil {
			m.multistate = InitialState
			return saveRFQ, 0, nil
		}
		if m.multistate == CopyInState {
			m.multistate = RunningState
			return saveCIn, 0, nil
		}

		m.multistate = RunningState
		return saveRd, 0, nil

	case CopyInState:
		return &pgproto3.CommandComplete{
			CommandTag: []byte{},
		}, 0, nil
	case RunningState:
		/* Step two: fetch all datarow messages */
		for i := range m.activeShards {
			// some shards may be in cc state

			if m.states[i] == ShardCCState {
				continue
			}
			if m.activeShards[i].Sync() == 0 {
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
				return nil, 0, err
			}

			switch msg.(type) {
			case *pgproto3.CommandComplete:
				//
				m.states[i] = ShardCCState
			case *pgproto3.ReadyForQuery:
				m.states[i] = ErrorState
				rollback()
				// sync is broken
				return nil, 0, ErrMultiShardSyncBroken
			default:
				return msg, uint(i), nil
			}
		}
		// all shard are in RFQ state
		m.multistate = CommandCompleteState
		return &pgproto3.CommandComplete{
			CommandTag: []byte{}, // XXX : fix this
		}, 0, nil
	case CommandCompleteState:
		spqrlog.Zero.Info().Msg("multishard server: enter rfq await mode")
		cntTXAct := 0
		cntUnSync := 0

		/* Step tree: fetch all datarow msgs */
		for i := range m.activeShards {
			// all shards shall be in cc state
			spqrlog.Zero.Info().Uint("shard", m.activeShards[i].ID()).Msg("multishard server: await server")

			if m.activeShards[i].Sync() == 0 {
				continue
			}

			cntUnSync++

			if m.states[i] != ShardCCState {
				return nil, 0, ErrMultiShardSyncBroken
			}

			if err := func() error {
				for {
					msg, err := m.activeShards[i].Receive()
					if err != nil {
						return err
					}

					switch q := msg.(type) {
					case *pgproto3.ReadyForQuery:
						if q.TxStatus == byte(txstatus.TXACT) {
							cntTXAct++
						}
						m.states[i] = ShardRFQState
						return nil
					default:
						// sync is broken
						return ErrMultiShardSyncBroken
					}
				}
			}(); err != nil {
				spqrlog.Zero.Info().
					Uint("shard", m.activeShards[i].ID()).
					Err(err).
					Msg("multishard server: encountered error while reading from shard")
				m.states[i] = ErrorState
				rollback()
				return nil, 0, err
			}
		}

		m.multistate = InitialState
		m.status = txstatus.TXIDLE
		switch cntTXAct {
		case 0:
			return &pgproto3.ReadyForQuery{
				TxStatus: byte(txstatus.TXIDLE), // XXX : fix this
			}, 0, nil
		case cntUnSync:
			return &pgproto3.ReadyForQuery{
				TxStatus: byte(txstatus.TXACT), // XXX : fix this
			}, 0, nil
		default:
			rollback()
			return nil, 0, fmt.Errorf("multishard server: unsync in tx status among shard connections")
		}
	}

	return nil, 0, nil
}

func (m *MultiShardServer) ReceiveShard(shardId uint) (pgproto3.BackendMessage, error) {
	for _, shard := range m.activeShards {
		if shard.ID() == shardId {
			m, err := shard.Receive()
			return m, err
		}
	}
	return nil, spqrerror.Newf(spqrerror.SPQR_NO_DATASHARD, "cannot find shard \"%d\"", shardId)
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
	if len(m.activeShards) == 0 {
		return txstatus.TXIDLE
	}
	/* should be equal beyond all connections */
	return m.activeShards[0].TxStatus()
}

func (m *MultiShardServer) Datashards() []shard.ShardHostInstance {
	return m.activeShards
}

var _ Server = &MultiShardServer{}
