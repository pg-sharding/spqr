package conn

import (
	"fmt"

	"github.com/shgo/src/internal/core"
	"github.com/shgo/src/internal/r"
)

const (
	SERVER_ACTIVE  = "ACTIVE"
	SERVER_PENDING = "PENDING"
)

type PoolingMode string

const (
	PoolingModeSession     = PoolingMode("SESSION")
	PoolingModeTransaction = PoolingMode("TRANSACTION")
)

type RelayState struct {
	TxActive bool

	ActiveShard int
}

type ConnManager interface {
	TXBeginCB(cl *core.ShClient, rst *RelayState) error
	TXEndCB(cl *core.ShClient, rst *RelayState) error

	RouteCB(cl *core.ShClient, rst *RelayState) error
	ValidateReRoute(rst *RelayState) bool
}

type TxConnManager struct {
}

func NewTxConnManager() *TxConnManager {
	return &TxConnManager{}
}

func (t *TxConnManager) RouteCB(cl *core.ShClient, rst *RelayState) error {

	shConn, err := cl.Route().GetConn("tcp6", rst.ActiveShard)

	if err != nil {
		return err
	}

	cl.AssignShrdConn(shConn)

	return nil
}

func (t *TxConnManager) ValidateReRoute(rst *RelayState) bool {
	return rst.ActiveShard == r.NOSHARD || rst.TxActive
}

func (t *TxConnManager) TXBeginCB(cl *core.ShClient, rst *RelayState) error {
	return nil
}

func (t *TxConnManager) TXEndCB(cl *core.ShClient, rst *RelayState) error {

	fmt.Println("releasing tx\n")

	cl.Route().Unroute(rst.ActiveShard, cl)

	return nil
}

var _ ConnManager = &TxConnManager{}

type SessConnManager struct {
}

func (s SessConnManager) TXBeginCB(cl *core.ShClient, rst *RelayState) error {
	return nil
}

func (s SessConnManager) TXEndCB(cl *core.ShClient, rst *RelayState) error {
	return nil
}

func (s SessConnManager) RouteCB(cl *core.ShClient, rst *RelayState) error {
	return nil
}

func (s SessConnManager) ValidateReRoute(rst *RelayState) bool {
	return false
}

var _ ConnManager = &SessConnManager{}

func NewSessConnManager() *SessConnManager {
	return &SessConnManager{}
}
