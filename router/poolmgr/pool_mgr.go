package poolmgr

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pkg/errors"
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

	ValidateReRoute(rst ConnectionKeeper) bool
	ConnectionActive(rst ConnectionKeeper) bool
}

// TODO : unit tests
func unRouteWithError(cmngr PoolMgr, client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	_ = cmngr.UnRouteCB(client, sh)
	return client.ReplyErr(errmsg)
}

func unRouteShardsCommon(cl client.RouterClient, sh []kr.ShardKey) error {
	var anyerr error
	anyerr = nil

	if cl.Server() == nil {
		/* If there is nothing to unroute, return */
		return nil
	}

	if cl.Server().TxStatus() != txstatus.TXIDLE {
		if err := cl.Server().Reset(); err != nil {
			return err
		}
		// TODO: figure out if we need this
		// return fmt.Errorf("failed to unroute client from connection with active TX")
	}

	for _, shkey := range sh {
		spqrlog.Zero.Debug().
			Uint("client", cl.ID()).
			Uint("shardn", spqrlog.GetPointer(cl.Server())).
			Str("key", shkey.Name).
			Msg("client unrouting from datashard")
		if err := cl.Server().UnRouteShard(shkey, cl.Rule()); err != nil {
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
func (t *TxConnManager) ValidateReRoute(rst ConnectionKeeper) bool {
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
func (s *SessConnManager) ValidateReRoute(rst ConnectionKeeper) bool {
	return rst.ActiveShards() == nil
}

func NewSessConnManager() *SessConnManager {
	return &SessConnManager{}
}

// TODO : unit tests
func MatchConnectionPooler(client client.RouterClient) (PoolMgr, error) {
	switch client.Rule().PoolMode {
	case config.PoolModeSession:
		return NewSessConnManager(), nil
	case config.PoolModeTransaction:
		return NewTxConnManager(), nil
	default:
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message:  fmt.Sprintf("unknown pool mode for route %v", client.ID()),
				Severity: "ERROR",
			},
		} {
			if err := client.Send(msg); err != nil {
				return nil, err
			}
		}

		return nil, errors.Errorf("unknown pool mode %v", client.Rule().PoolMode)
	}
}
