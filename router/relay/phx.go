package relay

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/router/poolmgr"
)

type XProtoStateHandler struct {
	cmngr poolmgr.PoolMgr
}

// ExecCommit implements ProtoStateHandler.
func (x *XProtoStateHandler) ExecCommit(rst RelayStateMgr, query string) error {

	rst.AddQuery(&pgproto3.Query{
		String: query,
	})
	ok, err := rst.ProcessMessageBuf(true, true, true, x.cmngr)
	if ok {
		rst.Client().Rollback()
	}
	return err
}

// ExecReset implements ProtoStateHandler.
func (*XProtoStateHandler) ExecReset(rst RelayStateMgr, msg pgproto3.FrontendMessage) error {
	panic("unimplemented")
}

// ExecResetMetadata implements ProtoStateHandler.
func (*XProtoStateHandler) ExecResetMetadata(rst RelayStateMgr, msg pgproto3.FrontendMessage, setting string) error {
	panic("unimplemented")
}

// ExecRollback implements ProtoStateHandler.
func (x *XProtoStateHandler) ExecRollback(rst RelayStateMgr, query string) error {

	rst.AddQuery(&pgproto3.Query{
		String: query,
	})
	ok, err := rst.ProcessMessageBuf(true, true, true, x.cmngr)
	if ok {
		rst.Client().Rollback()
	}
	return err
}

// ExecSet implements ProtoStateHandler.
func (*XProtoStateHandler) ExecSet(rst RelayStateMgr, msg pgproto3.FrontendMessage, name string, value string) error {
	panic("unimplemented")
}

// ExecSetLocal implements ProtoStateHandler.
func (*XProtoStateHandler) ExecSetLocal(rst RelayStateMgr, msg pgproto3.FrontendMessage) error {
	panic("unimplemented")
}

var _ ProtoStateHandler = &XProtoStateHandler{}

func NewXProtoStateHandler(cmngr poolmgr.PoolMgr) ProtoStateHandler {
	return &XProtoStateHandler{
		cmngr: cmngr,
	}
}