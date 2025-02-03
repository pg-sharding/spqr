package relay

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/parser"

	"github.com/pg-sharding/lyx/lyx"
)

type QueryStateExecutorImpl struct {
	txstatus.TxStatusMgr

	txStatus txstatus.TXStatus
	cl       client.RouterClient
}

func (s *QueryStateExecutorImpl) SetTxStatus(status txstatus.TXStatus) {
	s.txStatus = status
	/* handle implicit transactions - rollback all local state for params */
	s.cl.CleanupLocalSet()
}

func (s *QueryStateExecutorImpl) TxStatus() txstatus.TXStatus {
	return s.txStatus
}

func (s *QueryStateExecutorImpl) ExecBegin(rst RelayStateMgr, query string, st *parser.ParseStateTXBegin) error {
	// explicitly set silent query message, as it can differ from query begin in xporot
	rst.AddSilentQuery(&pgproto3.Query{
		String: query,
	})

	s.SetTxStatus(txstatus.TXACT)
	s.cl.StartTx()

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
func (s *QueryStateExecutorImpl) ExecCommit(rst RelayStateMgr, query string) error {
	// Virtual tx case. Do the whole logic locally
	if !rst.PoolMgr().ConnectionActive(rst) {
		s.cl.CommitActiveSet()
		_ = rst.Client().ReplyCommandComplete("COMMIT")
		s.SetTxStatus(txstatus.TXIDLE)
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
func (s *QueryStateExecutorImpl) ExecRollback(rst RelayStateMgr, query string) error {
	// Virtual tx case. Do the whole logic locally
	if !rst.PoolMgr().ConnectionActive(rst) {
		s.cl.Rollback()
		_ = s.cl.ReplyCommandComplete("ROLLBACK")
		s.SetTxStatus(txstatus.TXIDLE)
		/* empty message buf */
		rst.Flush()
		return nil
	}
	rst.AddQuery(&pgproto3.Query{
		String: query,
	})
	err := rst.ProcessMessageBuf(true, true)
	if err == nil {
		s.cl.Rollback()
	}
	return err
}

func (s *QueryStateExecutorImpl) ExecSet(rst RelayStateMgr, query string, name, value string) error {
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

func (s *QueryStateExecutorImpl) ExecReset(rst RelayStateMgr, query, setting string) error {
	if rst.PoolMgr().ConnectionActive(rst) {
		return rst.ProcessMessage(rst.Client().ConstructClientParams(), true, false)
	}
	return nil
}

func (s *QueryStateExecutorImpl) ExecResetMetadata(rst RelayStateMgr, query string, setting string) error {
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

func (s *QueryStateExecutorImpl) ExecSetLocal(rst RelayStateMgr, query, name, value string) error {
	if rst.PoolMgr().ConnectionActive(rst) {
		rst.AddQuery(&pgproto3.Query{String: query})
		return rst.ProcessMessageBuf(true, true)

	}
	return nil
}

var _ QueryStateExecutor = &QueryStateExecutorImpl{}

func NewQueryStateExecutor(cl client.RouterClient) QueryStateExecutor {
	return &QueryStateExecutorImpl{
		cl:       cl,
		txStatus: txstatus.TXIDLE,
	}
}
