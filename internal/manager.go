package internal

import (
	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pkg/errors"
)

type ConnManager interface {
	TXBeginCB(client Client, rst *RelayState) error
	TXEndCB(client Client, rst *RelayState) error

	RouteCB(client Client, sh []ShardKey) error
	UnRouteCB(client Client, sh []ShardKey) error
	UnRouteWithError(client Client, sh []ShardKey, errmsg string) error

	ValidateReRoute(rst *RelayState) bool
}

func unRouteWithError(cmngr ConnManager, client Client, sh []ShardKey, errmsg string) error {
	_ = cmngr.UnRouteCB(client, sh)

	return client.ReplyErr(errmsg)
}

type TxConnManager struct{}

func (t *TxConnManager) UnRouteWithError(client Client, sh []ShardKey, errmsg string) error {
	return unRouteWithError(t, client, sh, errmsg)
}

func (t *TxConnManager) UnRouteCB(cl Client, sh []ShardKey) error {
	for _, shkey := range sh {
		if err := cl.Server().UnrouteShard(shkey); err != nil {
			return err
		}
	}
	return cl.Unroute()
}

func NewTxConnManager() *TxConnManager {
	return &TxConnManager{}
}

func (t *TxConnManager) RouteCB(client Client, sh []ShardKey) error {

	for _, shkey := range sh {
		if err := client.Server().AddShard(shkey); err != nil {
			return err
		}
	}

	return nil
}

func (t *TxConnManager) ValidateReRoute(rst *RelayState) bool {
	return rst.ActiveShards == nil || !rst.TxActive
}

func (t *TxConnManager) TXBeginCB(client Client, rst *RelayState) error {
	return nil
}

func (t *TxConnManager) TXEndCB(client Client, rst *RelayState) error {

	for _, sh := range rst.ActiveShards {
		_ = client.Server().UnrouteShard(sh)
	}

	rst.ActiveShards = nil

	return nil
}

type SessConnManager struct{}

func (s *SessConnManager) UnRouteWithError(client Client, sh []ShardKey, errmsg string) error {
	return unRouteWithError(s, client, sh, errmsg)
}

func (s *SessConnManager) UnRouteCB(cl Client, sh []ShardKey) error {
	for _, shkey := range sh {
		if err := cl.Server().UnrouteShard(shkey); err != nil {
			return err
		}
	}

	return nil
}

func (s *SessConnManager) TXBeginCB(client Client, rst *RelayState) error {
	return nil
}

func (s *SessConnManager) TXEndCB(client Client, rst *RelayState) error {
	return nil
}

func (s *SessConnManager) RouteCB(client Client, sh []ShardKey) error {
	for _, shkey := range sh {
		if err := client.Server().AddShard(shkey); err != nil {
			return err
		}
	}

	return nil
}

func (s *SessConnManager) ValidateReRoute(rst *RelayState) bool {
	return rst.ActiveShards == nil
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
