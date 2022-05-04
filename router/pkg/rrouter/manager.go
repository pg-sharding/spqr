package rrouter

import (
	"fmt"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgproto3/v2"
	"github.com/pkg/errors"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/pkg/client"
)

type ConnManager interface {
	TXBeginCB(client client.RouterClient, rst RelayStateInteractor) error
	TXEndCB(client client.RouterClient, rst RelayStateInteractor) error

	RouteCB(client client.RouterClient, sh []kr.ShardKey) error
	UnRouteCB(client client.RouterClient, sh []kr.ShardKey) error
	UnRouteWithError(client client.RouterClient, sh []kr.ShardKey, errmsg error) error

	ValidateReRoute(rst RelayStateInteractor) bool
}

func unRouteWithError(cmngr ConnManager, client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	_ = cmngr.UnRouteCB(client, sh)

	return client.ReplyErrMsg(errmsg.Error())
}

type TxConnManager struct{}

func (t *TxConnManager) UnRouteWithError(client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	return unRouteWithError(t, client, sh, errmsg)
}

func (t *TxConnManager) UnRouteCB(cl client.RouterClient, sh []kr.ShardKey) error {
	for _, shkey := range sh {
		spqrlog.Logger.Printf(spqrlog.LOG, "unrouting from datashard %v", shkey.Name)
		if err := cl.Server().UnRouteShard(shkey); err != nil {
			_ = cl.Unroute()
			return err
		}
	}
	return cl.Unroute()
}

func NewTxConnManager() *TxConnManager {
	return &TxConnManager{}
}

func (t *TxConnManager) RouteCB(client client.RouterClient, sh []kr.ShardKey) error {

	for _, shkey := range sh {
		spqrlog.Logger.Printf(spqrlog.LOG, "adding datashard %v", shkey.Name)
		_ = client.ReplyNotice(fmt.Sprintf("adding datashard %v", shkey.Name))

		if err := client.Server().AddShard(shkey); err != nil {
			return err
		}
	}

	return nil
}

func (t *TxConnManager) ValidateReRoute(rst RelayStateInteractor) bool {
	return rst.ActiveShards() == nil || rst.TxStatus() == conn.TXIDLE
}

func (t *TxConnManager) TXBeginCB(client client.RouterClient, rst RelayStateInteractor) error {
	return nil
}

func (t *TxConnManager) TXEndCB(client client.RouterClient, rst RelayStateInteractor) error {
	ash := rst.ActiveShards()
	spqrlog.Logger.Printf(spqrlog.LOG, "end of tx unrouting from %v", ash)
	rst.ActiveShardsReset()

	if err := t.UnRouteCB(client, ash); err != nil {
		return err
	}

	return nil
}

type SessConnManager struct{}

func (s *SessConnManager) UnRouteWithError(client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	return unRouteWithError(s, client, sh, errmsg)
}

func (s *SessConnManager) UnRouteCB(cl client.RouterClient, sh []kr.ShardKey) error {
	for _, shkey := range sh {
		if err := cl.Server().UnRouteShard(shkey); err != nil {
			return err
		}
	}

	return nil
}

func (s *SessConnManager) TXBeginCB(client client.RouterClient, rst RelayStateInteractor) error {
	return nil
}

func (s *SessConnManager) TXEndCB(client client.RouterClient, rst RelayStateInteractor) error {
	return nil
}

func (s *SessConnManager) RouteCB(client client.RouterClient, sh []kr.ShardKey) error {
	for _, shkey := range sh {
		if err := client.Server().AddShard(shkey); err != nil {
			return err
		}
	}

	return nil
}

func (s *SessConnManager) ValidateReRoute(rst RelayStateInteractor) bool {
	return rst.ActiveShards() == nil
}

func NewSessConnManager() *SessConnManager {
	return &SessConnManager{}
}

func MatchConnectionPooler(client client.RouterClient) (ConnManager, error) {
	switch client.Rule().PoolingMode {
	case config.PoolingModeSession:
		return NewSessConnManager(), nil
	case config.PoolingModeTransaction:
		return NewTxConnManager(), nil
	default:
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message:  fmt.Sprintf("unknown pooling mode for route %v", client.ID()),
				Severity: "ERROR",
			},
		} {
			if err := client.Send(msg); err != nil {
				return nil, err
			}
		}

		return nil, errors.Errorf("unknown pooling mode %v", client.Rule().PoolingMode)
	}
}
