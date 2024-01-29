package relay

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/router/poolmgr"
)

type SimpleProtoStateHandler struct {
	cmngr poolmgr.PoolMgr
}

func (s *SimpleProtoStateHandler) ExecCommit(rst RelayStateMgr, msg pgproto3.FrontendMessage) error {
	if !s.cmngr.ConnectionActive(rst) {
		return fmt.Errorf("client relay has no connection to shards")
	}
	rst.AddQuery(msg)
	ok, err := rst.ProcessMessageBuf(true, true, s.cmngr)
	if ok {
		rst.Client().CommitActiveSet()
	}
	return err
}

func (s *SimpleProtoStateHandler) ExecRollback(rst RelayStateMgr, msg pgproto3.FrontendMessage) error {
	rst.AddQuery(msg)
	ok, err := rst.ProcessMessageBuf(true, true, s.cmngr)
	if ok {
		rst.Client().Rollback()
	}
	return err
}

func (s *SimpleProtoStateHandler) ExecSet(rst RelayStateMgr, msg pgproto3.FrontendMessage, name, value string) error {
	rst.AddQuery(msg)
	if ok, err := rst.ProcessMessageBuf(true, true, s.cmngr); err != nil {
		return err
	} else if ok {
		rst.Client().SetParam(name, value)
	}
	return nil
}

func (s *SimpleProtoStateHandler) ExecReset(rst RelayStateMgr, msg pgproto3.FrontendMessage) error {
	if s.cmngr.ConnectionActive(rst) {
		return rst.ProcessMessage(rst.Client().ConstructClientParams(), true, false, s.cmngr)
	}
	return nil
}

func (s *SimpleProtoStateHandler) ExecResetMetadata(rst RelayStateMgr, msg pgproto3.FrontendMessage, setting string) error {
	if !s.cmngr.ConnectionActive(rst) {

		return nil
	}
	rst.AddQuery(msg)
	_, err := rst.ProcessMessageBuf(true, true, s.cmngr)
	if err != nil {
		return err
	}

	rst.Client().ResetParam(setting)
	if setting == "session_authorization" {
		rst.Client().ResetParam("role")
	}
	return nil
}

func (s *SimpleProtoStateHandler) ExecSetLocal(rst RelayStateMgr, msg pgproto3.FrontendMessage) error {
	if s.cmngr.ConnectionActive(rst) {
		rst.AddQuery(msg)
		_, err := rst.ProcessMessageBuf(true, true, s.cmngr)
		return err
	}
	return nil
}

var _ ProtoStateHandler = &SimpleProtoStateHandler{}

func NewSimpleProtoStateHandler(cmngr poolmgr.PoolMgr) ProtoStateHandler {
	return &SimpleProtoStateHandler{
		cmngr: cmngr,
	}
}
