package poolmgr

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
)

type GangMgr interface {
	txstatus.TxStatusMgr

	ActiveShards() []kr.ShardKey
	ActiveShardsReset()

	SyncCount() int64

	DataPending() bool

	Client() client.RouterClient
}

type PoolMgr interface {
	TXEndCB(gangMgr GangMgr) error

	ValidateGangChange(gangMgr GangMgr) bool
	ConnectionActive(gangMgr GangMgr) bool
}

// TODO : unit tests
func UnrouteCommon(
	cl client.RouterClient,
	sh []kr.ShardKey) error {
	var anyerr error
	anyerr = nil

	serv := cl.Server()

	if serv == nil {
		/* If there is nothing to unroute, return */
		return nil
	}

	if serv.TxStatus() != txstatus.TXIDLE {
		if err := serv.Reset(); err != nil {
			return err
		}
		// TODO: figure out if we need this
		// return fmt.Errorf("failed to unroute client from connection with active TX")
	}

	for _, shkey := range sh {
		spqrlog.Zero.Debug().
			Uint("client", cl.ID()).
			Uint("shardn", spqrlog.GetPointer(serv)).
			Str("key", shkey.Name).
			Msg("client unrouting from datashard")
		if err := serv.UnRouteShard(shkey, cl.Rule()); err != nil {
			anyerr = err
		}
	}

	_ = cl.Unroute()

	return anyerr
}

type TxConnManager struct {
}

func NewTxConnManager() *TxConnManager {
	return &TxConnManager{}
}

// TODO : unit tests
func (t *TxConnManager) ConnectionActive(gangMgr GangMgr) bool {
	return gangMgr.ActiveShards() != nil
}

// TODO : unit tests
func (t *TxConnManager) ValidateGangChange(gangMgr GangMgr) bool {
	spqrlog.Zero.Debug().
		Uint("client", gangMgr.Client().ID()).
		Int("shards", len(gangMgr.ActiveShards())).
		Int64("sync-count", gangMgr.SyncCount()).
		Bool("data pending", gangMgr.DataPending()).
		Msg("client validate rerouting of TX")

	if gangMgr.SyncCount() != 0 || gangMgr.DataPending() {
		return false
	}

	return gangMgr.ActiveShards() == nil || gangMgr.TxStatus() == txstatus.TXIDLE
}

// TODO : unit tests
func (t *TxConnManager) TXEndCB(rst GangMgr) error {
	ash := rst.ActiveShards()
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Msg("client end of transaction, unrouting from active shards")
	rst.ActiveShardsReset()

	return UnrouteCommon(rst.Client(), ash)
}

type SessConnManager struct {
}

func (s *SessConnManager) TXEndCB(rst GangMgr) error {
	return nil
}

// TODO : unit tests
func (t *SessConnManager) ConnectionActive(rst GangMgr) bool {
	return rst.ActiveShards() != nil
}

// TODO : unit tests
func (s *SessConnManager) ValidateGangChange(rst GangMgr) bool {
	return rst.ActiveShards() == nil
}

func NewSessConnManager() *SessConnManager {
	return &SessConnManager{}
}

type VirtualConnManager struct {
}

// ConnectionActive implements PoolMgr.
func (v *VirtualConnManager) ConnectionActive(rst GangMgr) bool {
	return true
}

// TXEndCB implements PoolMgr.
func (v *VirtualConnManager) TXEndCB(rst GangMgr) error {
	return nil
}

// ValidateGangChange implements PoolMgr.
func (v *VirtualConnManager) ValidateGangChange(rst GangMgr) bool {
	return false
}

// TODO : unit tests
func MatchConnectionPooler(client client.RouterClient) (PoolMgr, error) {
	switch client.Rule().PoolMode {
	case config.PoolModeSession:
		return NewSessConnManager(), nil
	case config.PoolModeTransaction:
		return NewTxConnManager(), nil
	case config.PoolModeVirtual:
		return &VirtualConnManager{}, nil
	default:
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message:  fmt.Sprintf("unknown route pool mode for client %v", client.ID()),
				Severity: "ERROR",
			},
		} {
			if err := client.Send(msg); err != nil {
				return nil, err
			}
		}

		return nil, fmt.Errorf("unknown pool mode %v", client.Rule().PoolMode)
	}
}
