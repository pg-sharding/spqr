package core

import (
	"fmt"

	"github.com/shgo/src/internal/r"
)


type RelayState struct {
	TxActive bool

	ActiveShard int
}

type ConnManager interface {
	TXBeginCB(cl *ShClient, rst *RelayState) error
	TXEndCB(cl *ShClient, rst *RelayState) error

	RouteCB(cl *ShClient, rst *RelayState) error
	ValidateReRoute(rst *RelayState) bool
}

type TxConnManager struct {
}

func NewTxConnManager() *TxConnManager {
	return &TxConnManager{}
}

func (t *TxConnManager) RouteCB(cl *ShClient, rst *RelayState) error {

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

func (t *TxConnManager) TXBeginCB(cl *ShClient, rst *RelayState) error {
	return nil
}

func (t *TxConnManager) TXEndCB(cl *ShClient, rst *RelayState) error {

	fmt.Println("releasing tx\n")

	cl.Route().Unroute(rst.ActiveShard, cl)

	return nil
}

var _ ConnManager = &TxConnManager{}

type SessConnManager struct {
}

func (s SessConnManager) TXBeginCB(cl *ShClient, rst *RelayState) error {
	return nil
}

func (s SessConnManager) TXEndCB(cl *ShClient, rst *RelayState) error {
	return nil
}

func (s SessConnManager) RouteCB(cl *ShClient, rst *RelayState) error {
	return nil
}

func (s SessConnManager) ValidateReRoute(rst *RelayState) bool {
	return false
}

var _ ConnManager = &SessConnManager{}

func NewSessConnManager() *SessConnManager {
	return &SessConnManager{}
}
