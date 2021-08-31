package internal

import (
	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pkg/errors"
)

type RelayState struct {
	TxActive bool

	ActiveBackendConn Server
	ActiveShard       Shard
}

type ConnManager interface {
	TXBeginCB(client Client, rst *RelayState) error
	TXEndCB(client Client, rst *RelayState) error

	RouteCB(client Client, rst *RelayState) error
	UnRouteCB(client Client, rst *RelayState) error
	UnRouteWithError(client Client, rst *RelayState, errmsg string) error
	ValidateReRoute(rst *RelayState) bool
}

func unRouteWithError(cmngr ConnManager, client Client, rst *RelayState, errmsg string) error {
	_ = cmngr.UnRouteCB(client, rst)

	return client.ReplyErr(errmsg)
}

type TxConnManager struct {
}

func (t *TxConnManager) UnRouteWithError(client Client, rst *RelayState, errmsg string) error {
	return unRouteWithError(t, client, rst, errmsg)
}

func (t *TxConnManager) UnRouteCB(cl Client, rst *RelayState) error {
	if rst.ActiveShard != nil {
		return cl.Route().Unroute(rst.ActiveShard.Name(), cl)
	}
	return nil
}

func NewTxConnManager() *TxConnManager {
	return &TxConnManager{}
}

func (t *TxConnManager) RouteCB(client Client, rst *RelayState) error {

	shConn, err := client.Route().GetConn(rst.ActiveShard.Cfg().Proto, rst.ActiveShard)

	if err != nil {
		return err
	}

	client.AssignServerConn(shConn)

	return nil
}

func (t *TxConnManager) ValidateReRoute(rst *RelayState) bool {
	return rst.ActiveShard == nil || !rst.TxActive
}

func (t *TxConnManager) TXBeginCB(client Client, rst *RelayState) error {
	return nil
}

func (t *TxConnManager) TXEndCB(client Client, rst *RelayState) error {

	_ = client.Route().Unroute(rst.ActiveShard.Name(), client)
	rst.ActiveShard = nil

	return nil
}

type SessConnManager struct {
}

func (s *SessConnManager) UnRouteWithError(client Client, rst *RelayState, errmsg string) error {
	return unRouteWithError(s, client, rst, errmsg)
}

func (s *SessConnManager) UnRouteCB(cl Client, rst *RelayState) error {
	if rst.ActiveShard != nil {
		return cl.Route().Unroute(rst.ActiveShard.Name(), cl)
	}
	return nil
}

func (s *SessConnManager) TXBeginCB(client Client, rst *RelayState) error {
	return nil
}

func (s *SessConnManager) TXEndCB(client Client, rst *RelayState) error {
	return nil
}

func (s *SessConnManager) RouteCB(client Client, rst *RelayState) error {

	servConn, err := client.Route().GetConn(rst.ActiveShard.Cfg().Proto, rst.ActiveShard)

	if err != nil {
		return err
	}

	client.AssignServerConn(servConn)
	rst.ActiveBackendConn = servConn

	return nil
}

func (s *SessConnManager) ValidateReRoute(rst *RelayState) bool {
	return rst.ActiveShard == nil
}

func NewSessConnManager() *SessConnManager {
	return &SessConnManager{}
}

func InitClConnection(client Client) (ConnManager, error) {

	var connmanager ConnManager
	switch client.Rule().PoolingMode {
	case config.PoolingModeSession:
		connmanager = NewSessConnManager()
	case config.PoolingModeTransaction:
		connmanager = NewTxConnManager()
	default:
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message:  "unknown pooling mode for route",
				Severity: "ERROR",
			},
		} {
			if err := client.Send(msg); err != nil {
				return nil, err
			}
		}
		return nil, errors.Errorf("unknown pooling mode %v", client.Rule().PoolingMode)
	}

	return connmanager, nil
}
