package poolmgr

import (
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
)

type ConnectionKeeper interface {
	txstatus.TxStatusMgr
	ActiveShards() []kr.ShardKey
	ActiveShardsReset()

	SyncCount() int64

	DataPending() bool

	Client() client.RouterClient
}

type PoolMgr interface {
	TXEndCB(rst ConnectionKeeper) error

	UnRouteCB(client client.RouterClient, sh []kr.ShardKey) error
	UnRouteWithError(client client.RouterClient, sh []kr.ShardKey, errmsg error) error

	ValidateSliceChange(rst ConnectionKeeper) bool
	ConnectionActive(rst ConnectionKeeper) bool
}

// TODO : unit tests
func unRouteWithError(cmngr PoolMgr, client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	_ = cmngr.UnRouteCB(client, sh)
	return client.ReplyErr(errmsg)
}

// TODO : unit tests
func UnrouteCommon(
	pmgr PoolMgr,
	cl client.RouterClient,
	activeShards []kr.ShardKey,
	shkey []kr.ShardKey) ([]kr.ShardKey, error) {
	newActiveShards := make([]kr.ShardKey, 0)
	for _, el := range activeShards {
		if slices.IndexFunc(shkey, func(k kr.ShardKey) bool {
			return k == el
		}) == -1 {
			newActiveShards = append(newActiveShards, el)
		}
	}
	if err := pmgr.UnRouteCB(cl, shkey); err != nil {
		return nil, err
	}
	if len(newActiveShards) > 0 {
		activeShards = newActiveShards
	} else {
		activeShards = nil
	}

	return activeShards, nil
}

func unRouteShardsCommon(cl client.RouterClient, sh []kr.ShardKey) error {
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

// TODO : unit tests
func (t *TxConnManager) UnRouteWithError(client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	return unRouteWithError(t, client, sh, errmsg)
}

// TODO : unit tests
func (t *TxConnManager) UnRouteCB(cl client.RouterClient, sh []kr.ShardKey) error {
	return unRouteShardsCommon(cl, sh)
}

func NewTxConnManager() *TxConnManager {
	return &TxConnManager{}
}

// TODO : unit tests
func (t *TxConnManager) ConnectionActive(rst ConnectionKeeper) bool {
	return rst.ActiveShards() != nil
}

// TODO : unit tests
func (t *TxConnManager) ValidateSliceChange(rst ConnectionKeeper) bool {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Int("shards", len(rst.ActiveShards())).
		Int64("sync-count", rst.SyncCount()).
		Bool("data pending", rst.DataPending()).
		Msg("client validate rerouting of TX")

	if rst.SyncCount() != 0 || rst.DataPending() {
		return false
	}

	return rst.ActiveShards() == nil || rst.TxStatus() == txstatus.TXIDLE
}

// TODO : unit tests
func (t *TxConnManager) TXEndCB(rst ConnectionKeeper) error {
	ash := rst.ActiveShards()
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Msg("client end of transaction, unrouting from active shards")
	rst.ActiveShardsReset()

	return t.UnRouteCB(rst.Client(), ash)
}

type SessConnManager struct {
}

// TODO : unit tests
func (s *SessConnManager) UnRouteWithError(client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	return unRouteWithError(s, client, sh, errmsg)
}

// TODO : unit tests
func (s *SessConnManager) UnRouteCB(cl client.RouterClient, sh []kr.ShardKey) error {
	return unRouteShardsCommon(cl, sh)
}

func (s *SessConnManager) TXEndCB(rst ConnectionKeeper) error {
	return nil
}

// TODO : unit tests
func (t *SessConnManager) ConnectionActive(rst ConnectionKeeper) bool {
	return rst.ActiveShards() != nil
}

// TODO : unit tests
func (s *SessConnManager) ValidateSliceChange(rst ConnectionKeeper) bool {
	return rst.ActiveShards() == nil
}

func NewSessConnManager() *SessConnManager {
	return &SessConnManager{}
}

type VirtualConnManager struct {
}

// ConnectionActive implements PoolMgr.
func (v *VirtualConnManager) ConnectionActive(rst ConnectionKeeper) bool {
	return true
}

// TXEndCB implements PoolMgr.
func (v *VirtualConnManager) TXEndCB(rst ConnectionKeeper) error {
	return nil
}

// UnRouteCB implements PoolMgr.
func (v *VirtualConnManager) UnRouteCB(client client.RouterClient, sh []kr.ShardKey) error {
	return nil
}

// UnRouteWithError implements PoolMgr.
func (v *VirtualConnManager) UnRouteWithError(client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	return unRouteWithError(v, client, sh, errmsg)
}

// ValidateSliceChange implements PoolMgr.
func (v *VirtualConnManager) ValidateSliceChange(rst ConnectionKeeper) bool {
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
