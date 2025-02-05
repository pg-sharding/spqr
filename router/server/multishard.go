package server

import (
	"fmt"
	"io"
	"slices"

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

type ServState int

const (
	ServerActiveState = ServState(iota)
	ServerRFQState
	ErrorState
	ErrorStateRFQ
	TerminalState /* after unrecoverable error */
)

type ShardsSliceServer struct {
	activeShards []shard.Shard

	sliceShards []kr.ShardKey

	multistate ServState

	pool *pool.DBPool

	cmdTag []byte

	RDsend      bool
	statusValid bool
	status      txstatus.TXStatus
}

func NewMultiShardServer(pool *pool.DBPool) (Server, error) {
	ret := &ShardsSliceServer{
		pool:         pool,
		activeShards: []shard.Shard{},
		sliceShards:  []kr.ShardKey{},
	}

	return ret, nil
}

// HasPrepareStatement implements Server.
func (m *ShardsSliceServer) HasPrepareStatement(hash uint64, shardId uint) (bool, *prepstatement.PreparedStatementDescriptor) {
	for _, shard := range m.activeShards {
		if shard.ID() == shardId {
			return shard.HasPrepareStatement(hash, shardId)
		}
	}
	return false, nil
}

// StorePrepareStatement implements Server.
func (m *ShardsSliceServer) StorePrepareStatement(hash uint64, shardId uint, d *prepstatement.PreparedStatementDefinition, rd *prepstatement.PreparedStatementDescriptor) error {
	for _, shard := range m.activeShards {
		if shard.ID() == shardId {
			return shard.StorePrepareStatement(hash, shardId, d, rd)
		}
	}
	return spqrerror.Newf(spqrerror.SPQR_NO_DATASHARD, "shard \"%d\" not found", shardId)
}

// DataPending implements Server.
func (m *ShardsSliceServer) DataPending() bool {
	for _, shard := range m.activeShards {
		if shard.DataPending() {
			return true
		}
	}
	return false
}

func (m *ShardsSliceServer) Reset() error {
	return nil
}

func (m *ShardsSliceServer) AddDataShard(clid uint, shkey kr.ShardKey, tsa tsa.TSA) error {
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
	m.multistate = ServerRFQState

	return nil
}

func (m *ShardsSliceServer) UnRouteShard(sh kr.ShardKey, rule *config.FrontendRule) error {
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

func (m *ShardsSliceServer) Name() string {
	return "multishard(sliced executor)"
}

func (m *ShardsSliceServer) connectSlice() error {
	for _, shkey := range m.sliceShards {
		if slices.ContainsFunc(m.activeShards, func(e shard.Shard) bool {
			return e.SHKey() == shkey
		}) {
			continue
		}
		sh, err := m.pool.Connection(0, shkey)
		if err != nil {
			m.multistate = TerminalState
			return err
		}
		m.activeShards = append(m.activeShards, sh)
	}

	return nil
}

func (m *ShardsSliceServer) Send(msg pgproto3.FrontendMessage) error {
	switch m.multistate {
	case TerminalState:
		m.rollbackState()
		return io.ErrUnexpectedEOF
	case ServerActiveState:
		m.multistate = TerminalState
		return fmt.Errorf("deployig in active state is not suported")
	}
	if err := m.connectSlice(); err != nil {
		return err
	}
	m.multistate = ServerActiveState
	m.statusValid = false
	m.RDsend = false

	for _, shard := range m.activeShards {
		spqrlog.Zero.Debug().
			Uint("shard", shard.ID()).
			Interface("message", msg).
			Msg("sending message to shard")
		if err := shard.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error while sending to shard")
			m.multistate = TerminalState
			return err
		}
	}

	return nil
}

func (m *ShardsSliceServer) SendShard(msg pgproto3.FrontendMessage, shardId uint) error {
	switch m.multistate {
	case TerminalState:
		m.rollbackState()
		return io.ErrUnexpectedEOF
	case ServerActiveState:
		m.multistate = TerminalState
		return fmt.Errorf("deployig in active state is not suported")
	}
	if err := m.connectSlice(); err != nil {
		return err
	}
	m.multistate = ServerActiveState
	m.statusValid = false
	m.RDsend = false

	if err := m.connectSlice(); err != nil {
		return err
	}

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
			spqrlog.Zero.Error().Err(err).Msg("error dispatching msg to server")
			m.multistate = TerminalState
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

func (m *ShardsSliceServer) rollbackState() {
	for i := range m.activeShards {
		spqrlog.Zero.Debug().
			Uint("shard", m.activeShards[i].ID()).
			Msg("rollback shard in multishard after error")
		m.pool.Put(m.activeShards[i])
	}

	m.activeShards = nil
}

func (m *ShardsSliceServer) Receive() (pgproto3.BackendMessage, error) {

	switch m.multistate {
	case TerminalState:
		m.rollbackState()
		return nil, io.ErrUnexpectedEOF
	case ErrorState:
		m.multistate = ErrorStateRFQ
		return &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Message:  "current transaction is aborted, ignoring statement",
		}, nil
	case ErrorStateRFQ:
		m.multistate = ErrorState
		return &pgproto3.ReadyForQuery{
			TxStatus: byte(txstatus.TXERR),
		}, nil
	case ServerActiveState:
		for _, sh := range m.activeShards {
			if sh.Sync() == 0 {
				continue
			}

		receiveLoop:
			for {
				if sh.Sync() == 0 {
					m.rollbackState()
					m.multistate = TerminalState
					return nil, MultiShardSyncBroken
				}
				msg, err := sh.Receive()
				if err != nil {
					m.multistate = TerminalState
					return nil, err
				}

				switch v := msg.(type) {
				case *pgproto3.RowDescription:
					if m.RDsend {
						continue
					}
					m.RDsend = true
					return msg, nil
				case *pgproto3.DataRow:
					return v, err
				case *pgproto3.CommandComplete:
					m.cmdTag = v.CommandTag
				case *pgproto3.ErrorResponse:
					m.multistate = ErrorStateRFQ
					return msg, nil
				case *pgproto3.ReadyForQuery:
					if m.statusValid {

						if v.TxStatus != byte(m.status) {
							m.rollbackState()
							m.multistate = TerminalState
							return nil, MultiShardSyncBroken
						}

						m.status = txstatus.TXStatus(v.TxStatus)

					} else {
						m.statusValid = true
						m.status = txstatus.TXStatus(v.TxStatus)
					}

					break receiveLoop
				default:
					m.rollbackState()
					m.multistate = TerminalState
					return nil, MultiShardSyncBroken
				}
			}
		}

		m.multistate = ServerRFQState
		return &pgproto3.CommandComplete{CommandTag: m.cmdTag}, nil
	case ServerRFQState:
		return &pgproto3.ReadyForQuery{
			TxStatus: byte(m.status),
		}, nil
	default:
		m.rollbackState()
		m.multistate = TerminalState
		return nil, MultiShardSyncBroken
	}
}

func (m *ShardsSliceServer) ReceiveShard(shardId uint) (pgproto3.BackendMessage, error) {
	if m.multistate == TerminalState {
		return nil, io.ErrUnexpectedEOF
	}

	for _, shard := range m.activeShards {
		if shard.ID() == shardId {
			return shard.Receive()
		}
	}
	return nil, spqrerror.Newf(spqrerror.SPQR_NO_DATASHARD, "cannot find shard \"%d\"", shardId)
}

func (m *ShardsSliceServer) Cleanup(rule config.FrontendRule) error {

	for _, sh := range m.activeShards {
		_ = sh.Cleanup(&rule)
		m.pool.Put(sh)
	}

	m.activeShards = nil
	m.multistate = ServerRFQState

	return nil
}

func (m *ShardsSliceServer) Sync() int64 {
	switch m.multistate {
	case TerminalState, ErrorState, ErrorStateRFQ:
		return -1
	}
	var syncCount int64
	for _, shard := range m.activeShards {
		syncCount += shard.Sync()
	}
	return syncCount
}

func (m *ShardsSliceServer) Cancel() error {
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

func (m *ShardsSliceServer) SetTxStatus(tx txstatus.TXStatus) {
	m.status = tx
}

func (m *ShardsSliceServer) TxStatus() txstatus.TXStatus {
	return m.status
}

func (m *ShardsSliceServer) Datashards() []shard.Shard {
	return m.activeShards
}

var _ Server = &ShardsSliceServer{}
