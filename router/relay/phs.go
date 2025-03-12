package relay

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/parser"
	"github.com/pg-sharding/spqr/router/pgcopy"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/pg-sharding/spqr/router/twopc"

	"github.com/pg-sharding/lyx/lyx"
)

type QueryStateExecutorImpl struct {
	txstatus.TxStatusMgr

	txStatus txstatus.TXStatus
	cl       client.RouterClient

	savedBegin *pgproto3.Query
}

var unexpectedDeployTxErr = fmt.Errorf("unexpected executor tx state in transaction deploy")
var unroutedClientDeployError = fmt.Errorf("failed to deploy tx status for unrouted client")

func (s *QueryStateExecutorImpl) deployTxStatusInternal(serv server.Server, q *pgproto3.Query, expTx txstatus.TXStatus) error {
	if serv == nil {
		return unroutedClientDeployError
	}

	if s.txStatus == txstatus.TXIDLE {
		/* unexpected? */
		return unexpectedDeployTxErr
	}

	for _, sh := range serv.Datashards() {
		st, err := shard.DeployTxOnShard(sh, q, expTx)

		if err != nil {
			/* assert st == txtstatus.TXERR? */
			s.SetTxStatus(txstatus.TXStatus(txstatus.TXERR))
			return err
		}

		s.SetTxStatus(txstatus.TXStatus(st))
	}

	return nil
}

// Deploy implements QueryStateExecutor.
func (s *QueryStateExecutorImpl) Deploy(server server.Server) error {
	if s.txStatus == txstatus.TXIDLE {
		/* unexpected? */
		return nil
	}

	if !s.cl.EnhancedMultiShardProcessing() {
		/* move this logic to executor */
		if s.TxStatus() == txstatus.TXACT && len(server.Datashards()) > 1 {
			return fmt.Errorf("cannot route in an active transaction")
		}
	}

	return s.deployTxStatusInternal(server, s.savedBegin, txstatus.TXACT)
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
	// explicitly set silent query message, as it can differ from query begin in xproto

	s.SetTxStatus(txstatus.TXACT)
	s.cl.StartTx()
	s.savedBegin = &pgproto3.Query{String: query}

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

	spqrlog.Zero.Debug().Uint("client", s.cl.ID()).Str("commit strategy", s.cl.CommitStrategy()).Msg("execute commit")

	if s.cl.CommitStrategy() == twopc.COMMIT_STRATEGY_2PC && len(s.Client().Server().Datashards()) > 1 {
		if err := twopc.ExecuteTwoPhaseCommit(s.cl.ID(), s.Client().Server()); err != nil {
			return err
		}
	} else {
		if err := s.deployTxStatusInternal(s.Client().Server(),
			&pgproto3.Query{String: query}, txstatus.TXIDLE); err != nil {
			return err
		}
	}

	rst.Client().CommitActiveSet()
	return rst.Client().ReplyCommandComplete("COMMIT")
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

	/* unroute will take care of tx server */
	s.SetTxStatus(txstatus.TXIDLE)
	if server := s.cl.Server(); server != nil {
		for _, sh := range server.Datashards() {
			if err := sh.Cleanup(&config.FrontendRule{
				PoolRollback: true,
			}); err != nil {
				return err
			}
		}
	}
	s.cl.Rollback()
	return s.cl.ReplyCommandComplete("ROLLBACK")
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

// TODO: unit tests
func (s *QueryStateExecutorImpl) ProcCopyPrepare(ctx context.Context, mgr meta.EntityMgr, stmt *lyx.Copy) (*pgcopy.CopyState, error) {
	spqrlog.Zero.Debug().
		Uint("client", s.cl.ID()).
		Msg("client pre-process copy")

	var relname string

	switch q := stmt.TableRef.(type) {
	case *lyx.RangeVar:
		relname = q.RelationName
	}
	// Read delimiter from COPY options
	delimiter := byte('\t')
	for _, opt := range stmt.Options {
		o := opt.(*lyx.Option)
		if strings.ToLower(o.Name) == "delimiter" {
			delimiter = o.Arg.(*lyx.AExprSConst).Value[0]
		}
		if strings.ToLower(o.Name) == "format" {
			if o.Arg.(*lyx.AExprSConst).Value == "csv" {
				delimiter = ','
			}
		}
	}

	/* If 'execute on' is specified or explicit tx is going, then no routing */
	if s.cl.ExecuteOn() != "" || s.TxStatus() == txstatus.TXACT {
		return &pgcopy.CopyState{
			Delimiter: delimiter,
			Attached:  true,
		}, nil
	}

	// TODO: check by whole RFQN
	ds, err := mgr.GetRelationDistribution(ctx, relname)
	if err != nil {
		return nil, err
	}
	if ds.Id == distributions.REPLICATED {
		return &pgcopy.CopyState{
			Scatter: true,
		}, nil
	}

	TargetType := ds.ColTypes[0]

	if len(ds.ColTypes) != 1 {
		return nil, fmt.Errorf("multi-column copy processing is not yet supported")
	}

	var hashFunc hashfunction.HashFunctionType

	if v, err := hashfunction.HashFunctionByName(ds.Relations[relname].DistributionKey[0].HashFunction); err != nil {
		return nil, err
	} else {
		hashFunc = v
	}

	krs, err := mgr.ListKeyRanges(ctx, ds.Id)
	if err != nil {
		return nil, err
	}

	colOffset := -1
	for indx, c := range stmt.Columns {
		if c == ds.Relations[relname].DistributionKey[0].Column {
			colOffset = indx
			break
		}
	}
	if colOffset == -1 {
		return nil, fmt.Errorf("failed to resolve target copy column offset")
	}

	return &pgcopy.CopyState{
		Delimiter:  delimiter,
		Krs:        krs,
		RM:         rmeta.NewRoutingMetadataContext(s.cl, mgr),
		TargetType: TargetType,
		HashFunc:   hashFunc,
	}, nil
}

// TODO : unit tests
func (s *QueryStateExecutorImpl) ProcCopy(ctx context.Context, data *pgproto3.CopyData, cps *pgcopy.CopyState) ([]byte, error) {
	if cps.Attached {
		for _, sh := range s.cl.Server().Datashards() {
			err := sh.Send(data)
			return nil, err
		}
		return nil, fmt.Errorf("metadata corrupted")
	}

	/* We dont really need to parse and route tuples for DISTRIBUTED relations */
	if cps.Scatter {
		return nil, s.cl.Server().Send(data)
	}

	var leftoverMsgData []byte = nil

	rowsMp := map[string][]byte{}

	values := make([]interface{}, 0)

	// Parse data
	// and decide where to route
	prevDelimiter := 0
	prevLine := 0
	currentAttr := 0

	for i, b := range data.Data {
		if i+2 < len(data.Data) && string(data.Data[i:i+2]) == "\\." {
			prevLine = len(data.Data)
			break
		}
		if b == '\n' || b == cps.Delimiter {

			if currentAttr == cps.ColumnOffset {
				tmp, err := hashfunction.ApplyHashFunctionOnStringRepr(data.Data[prevDelimiter:i], cps.TargetType, cps.HashFunc)
				if err != nil {
					return nil, err
				}
				values = append(values, tmp)
			}

			currentAttr++
			prevDelimiter = i + 1
		}
		if b != '\n' {
			continue
		}

		// check where this tuple should go
		currroute, err := cps.RM.DeparseKeyWithRangesInternal(ctx, values, cps.Krs)
		if err != nil {
			return nil, err
		}

		if currroute == nil {
			return nil, fmt.Errorf("multishard copy is not supported: %+v at line number %d %d %v", values[0], prevLine, i, b)
		}

		values = nil
		rowsMp[currroute.Name] = append(rowsMp[currroute.Name], data.Data[prevLine:i+1]...)
		currentAttr = 0
		prevLine = i + 1
	}

	if prevLine != len(data.Data) {
		if spqrlog.IsDebugLevel() {
			_ = s.cl.ReplyNotice(fmt.Sprintf("leftover data saved to next iter %d - %d", prevLine, len(data.Data)))
		}
		leftoverMsgData = data.Data[prevLine:len(data.Data)]
	}

	for _, sh := range s.cl.Server().Datashards() {
		if bts, ok := rowsMp[sh.Name()]; ok {
			err := sh.Send(&pgproto3.CopyData{Data: bts})
			if err != nil {
				return nil, err
			}
		}
	}

	// shouldn't exit from here
	return leftoverMsgData, nil
}

// TODO : unit tests
func (s *QueryStateExecutorImpl) ProcCopyComplete(query pgproto3.FrontendMessage) error {
	spqrlog.Zero.Debug().
		Uint("client", s.cl.ID()).
		Type("query-type", query).
		Msg("client process copy end")
	server := s.cl.Server()
	/* non-null server should never be set to null here until we call Unroute()
	in complete relay */
	if server == nil {
		return fmt.Errorf("client not routed in copy complete phase, resetting")
	}
	if err := server.Send(query); err != nil {
		return err
	}

	for {
		msg, err := server.Receive()
		if err != nil {
			return err
		}
		switch msg.(type) {
		case *pgproto3.CommandComplete, *pgproto3.ErrorResponse:
			return s.cl.Send(msg)
		default:
			if err := s.cl.Send(msg); err != nil {
				return err
			}
		}
	}
}

// TODO : unit tests
func (s *QueryStateExecutorImpl) ProcQuery(qd *QueryDesc, mgr meta.EntityMgr, waitForResp bool, replyCl bool) ([]pgproto3.BackendMessage, error) {
	serv := s.Client().Server()

	if serv == nil {
		return nil, fmt.Errorf("client %p is out of transaction sync with router", s.Client())
	}

	spqrlog.Zero.Debug().
		Uints("shards", shard.ShardIDs(serv.Datashards())).
		Type("query-type", qd.Msg).
		Msg("relay process query")

	if qd.P == nil {
		if err := serv.Send(qd.Msg); err != nil {
			return nil, err
		}

		if s.Client().ShowNoticeMsg() && replyCl {
			_ = replyShardMatches(s.Client(), server.ServerShkeys(serv))
		}
	} else {
		et := qd.P.ExecutionTargets()

		if len(et) == 0 {
			if err := serv.Send(qd.Msg); err != nil {
				return nil, err
			}

			if s.Client().ShowNoticeMsg() && replyCl {
				_ = replyShardMatches(s.Client(), server.ServerShkeys(serv))
			}
		} else {
			for _, targ := range et {
				if err := serv.SendShard(qd.Msg, targ); err != nil {
					return nil, err
				}
			}

			if s.Client().ShowNoticeMsg() && replyCl {
				_ = replyShardMatches(s.Client(), et)
			}
		}
	}

	waitForRespLocal := waitForResp

	switch qd.Msg.(type) {
	case *pgproto3.Query:
		// ok
	case *pgproto3.Sync:
		// ok
	default:
		waitForRespLocal = false
	}

	if !waitForRespLocal {
		/* we do not alter txstatus here */
		return nil, nil
	}

	unreplied := make([]pgproto3.BackendMessage, 0)

	for {
		msg, err := serv.Receive()
		if err != nil {
			return nil, err
		}

		switch v := msg.(type) {
		case *pgproto3.CopyInResponse:
			// handle replyCl somehow
			err = s.Client().Send(msg)
			if err != nil {
				return nil, err
			}

			q := qd.Stmt.(*lyx.Copy)

			if err := func() error {
				var leftoverMsgData []byte
				ctx := context.TODO()

				cps, err := s.ProcCopyPrepare(ctx, mgr, q)
				if err != nil {
					return err
				}

				for {
					cpMsg, err := s.Client().Receive()
					if err != nil {
						return err
					}

					switch newMsg := cpMsg.(type) {
					case *pgproto3.CopyData:
						leftoverMsgData = append(leftoverMsgData, newMsg.Data...)

						if leftoverMsgData, err = s.ProcCopy(ctx, &pgproto3.CopyData{Data: leftoverMsgData}, cps); err != nil {
							/* complete relay if copy failed here */
							return err
						}
					case *pgproto3.CopyDone, *pgproto3.CopyFail:
						return s.ProcCopyComplete(cpMsg)
					default:
						/* panic? */
					}
				}
			}(); err != nil {
				return nil, err
			}
		case *pgproto3.ReadyForQuery:
			s.SetTxStatus(txstatus.TXStatus(v.TxStatus))
			return unreplied, nil
		case *pgproto3.ErrorResponse:

			spqrlog.Zero.Debug().
				Str("server", serv.Name()).
				Type("msg-type", v).
				Msg("received message from server")

			if replyCl {
				err = s.Client().Send(msg)
				if err != nil {
					return nil, err
				}
			} else {
				unreplied = append(unreplied, msg)
			}
		// never resend these msgs
		case *pgproto3.ParseComplete:
			unreplied = append(unreplied, msg)
		case *pgproto3.BindComplete:
			unreplied = append(unreplied, msg)
		default:
			spqrlog.Zero.Debug().
				Str("server", serv.Name()).
				Type("msg-type", v).
				Msg("received message from server")
			if replyCl {
				err = s.Client().Send(msg)
				if err != nil {
					return nil, err
				}
			} else {
				unreplied = append(unreplied, msg)
			}
		}
	}
}

func (s *QueryStateExecutorImpl) Client() client.RouterClient {
	return s.cl
}

var _ QueryStateExecutor = &QueryStateExecutorImpl{}

func NewQueryStateExecutor(cl client.RouterClient) QueryStateExecutor {
	return &QueryStateExecutorImpl{
		cl:       cl,
		txStatus: txstatus.TXIDLE,
	}
}
