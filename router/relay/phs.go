package relay

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/parser"

	"github.com/pg-sharding/lyx/lyx"
)

type QueryStateExecutor struct {
}

func (s *QueryStateExecutor) ExecBegin(rst RelayStateMgr, query string, st *parser.ParseStateTXBegin) error {
	// explicitly set silent query message, as it can differ from query begin in xporot
	rst.AddSilentQuery(&pgproto3.Query{
		String: query,
	})

	rst.SetTxStatus(txstatus.TXACT)
	rst.Client().StartTx()

	spqrlog.Zero.Debug().Msg("start new transaction")

	for _, opt := range st.Options {
		switch opt {
		case lyx.TransactionReadOnly:
			rst.Client().SetTsa(config.TargetSessionAttrsPS)
		case lyx.TransactionReadWrite:
			rst.Client().SetTsa(config.TargetSessionAttrsRW)
		}
	}
	return rst.Client().ReplyCommandComplete("BEGIN")

}

// query in commit query. maybe commit or commit `name`
func (s *QueryStateExecutor) ExecCommit(rst RelayStateMgr, query string) error {
	// Virtual tx case. Do the whole logic locally
	if !rst.PoolMgr().ConnectionActive(rst) {
		rst.Client().CommitActiveSet()
		_ = rst.Client().ReplyCommandComplete("COMMIT")
		rst.SetTxStatus(txstatus.TXIDLE)
		/* empty message buf */
		rst.Flush()
		return nil
	}
	rst.AddQuery(&pgproto3.Query{
		String: query,
	})
	err := rst.ProcessMessageBuf(true, true)
	if err == nil {
		rst.Client().CommitActiveSet()
	}
	return err
}

/* TODO: proper support for rollback to savepoint */
func (s *QueryStateExecutor) ExecRollback(rst RelayStateMgr, query string) error {
	// Virtual tx case. Do the whole logic locally
	if !rst.PoolMgr().ConnectionActive(rst) {
		rst.Client().Rollback()
		_ = rst.Client().ReplyCommandComplete("ROLLBACK")
		rst.SetTxStatus(txstatus.TXIDLE)
		/* empty message buf */
		rst.Flush()
		return nil
	}
	rst.AddQuery(&pgproto3.Query{
		String: query,
	})
	err := rst.ProcessMessageBuf(true, true)
	if err == nil {
		rst.Client().Rollback()
	}
	return err
}

func (s *QueryStateExecutor) ExecSet(rst RelayStateMgr, query string, name, value string) error {
	if len(name) == 0 {
		// some session characteristic, ignore
		return rst.Client().ReplyCommandComplete("SET")
	}
	if !rst.PoolMgr().ConnectionActive(rst) {
		rst.Client().SetParam(name, value)
		return rst.Client().ReplyCommandComplete("SET")
	}
	spqrlog.Zero.Debug().Str("name", name).Str("value", value).Msg("execute set query")
	rst.AddQuery(&pgproto3.Query{String: query})
	if err := rst.ProcessMessageBuf(true, true); err != nil {
		return err
	}
	rst.Client().SetParam(name, value)

	return nil
}

func (s *QueryStateExecutor) ExecReset(rst RelayStateMgr, query, setting string) error {
	if rst.PoolMgr().ConnectionActive(rst) {
		return rst.ProcessMessage(rst.Client().ConstructClientParams(), true, false)
	}
	return nil
}

func (s *QueryStateExecutor) ExecResetMetadata(rst RelayStateMgr, query string, setting string) error {
	if !rst.PoolMgr().ConnectionActive(rst) {
		return nil
	}
	rst.AddQuery(&pgproto3.Query{String: query})

	if err := rst.ProcessMessageBuf(true, true); err != nil {
		return err
	}

	rst.Client().ResetParam(setting)
	if setting == "session_authorization" {
		rst.Client().ResetParam("role")
	}
	return nil
}

func (s *QueryStateExecutor) ExecSetLocal(rst RelayStateMgr, query, name, value string) error {
	if rst.PoolMgr().ConnectionActive(rst) {
		rst.AddQuery(&pgproto3.Query{String: query})
		return rst.ProcessMessageBuf(true, true)

	}
	return nil
}

var _ ProtoStateHandler = &QueryStateExecutor{}

func NewSimpleProtoStateHandler() ProtoStateHandler {
	return &QueryStateExecutor{}
}
