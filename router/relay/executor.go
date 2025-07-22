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
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
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

var (
	errUnAttached = fmt.Errorf("client is not currently attached to server")
)

func (s *QueryStateExecutorImpl) deployTxStatusInternal(serv server.Server, q *pgproto3.Query, expTx txstatus.TXStatus) error {
	if serv == nil {
		return fmt.Errorf("failed to deploy tx status for unrouted client")
	}

	if s.txStatus == txstatus.TXIDLE {
		/* unexpected? */
		return fmt.Errorf("unexpected executor tx state in transaction deploy")
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
	if server == nil {
		return errUnAttached
	}
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

func (s *QueryStateExecutorImpl) DeployTx(server server.Server, query string) error {
	s.SetTxStatus(txstatus.TXACT)
	s.savedBegin = &pgproto3.Query{String: query}

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
	if rst.PoolMgr().ConnectionActive(rst) {
		return rst.QueryExecutor().DeployTx(rst.Client().Server(), query)
	}

	s.SetTxStatus(txstatus.TXACT)
	s.cl.StartTx()

	// explicitly set silent query message, as it can differ from query begin in xproto
	s.savedBegin = &pgproto3.Query{String: query}

	spqrlog.Zero.Debug().Uint("client", rst.Client().ID()).Msg("start new transaction")

	for _, opt := range st.Options {
		switch opt {
		case lyx.TransactionReadOnly:
			rst.Client().SetTsa(false, config.TargetSessionAttrsPS)
		case lyx.TransactionReadWrite:
			rst.Client().SetTsa(false, config.TargetSessionAttrsRW)
		default:
			rst.Client().SetTsa(false, config.TargetSessionAttrsRW)
		}
	}
	return rst.Client().ReplyCommandComplete("BEGIN")
}

func (s *QueryStateExecutorImpl) ExecCommitTx(query string) error {
	spqrlog.Zero.Debug().Uint("client", s.cl.ID()).Str("commit strategy", s.cl.CommitStrategy()).Msg("execute commit")

	serv := s.cl.Server()

	if s.cl.CommitStrategy() == twopc.COMMIT_STRATEGY_2PC && len(serv.Datashards()) > 1 {
		if err := twopc.ExecuteTwoPhaseCommit(s.cl.ID(), serv); err != nil {
			return err
		}
	} else {
		if err := s.deployTxStatusInternal(serv,
			&pgproto3.Query{String: query}, txstatus.TXIDLE); err != nil {
			return err
		}
	}
	return nil
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

	if err := s.ExecCommitTx(query); err != nil {
		return err
	}

	rst.Client().CommitActiveSet()
	return rst.Client().ReplyCommandComplete("COMMIT")
}

func (s *QueryStateExecutorImpl) ExecRollbackServer() error {
	if server := s.cl.Server(); server != nil {
		for _, sh := range server.Datashards() {
			if err := sh.Cleanup(&config.FrontendRule{
				PoolRollback: true,
			}); err != nil {
				return err
			}
		}
	}
	s.SetTxStatus(txstatus.TXIDLE)
	return nil
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
	if err := s.ExecRollbackServer(); err != nil {
		return err
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
	if err := rst.ProcessSimpleQuery(&pgproto3.Query{String: query}, true); err != nil {
		return err
	}
	rst.Client().SetParam(name, value)

	return nil
}

func (s *QueryStateExecutorImpl) ExecReset(rst RelayStateMgr, query, setting string) error {
	if rst.PoolMgr().ConnectionActive(rst) {
		return rst.ProcessSimpleQuery(rst.Client().ConstructClientParams(), false)
	}
	return nil
}

func (s *QueryStateExecutorImpl) ExecResetMetadata(rst RelayStateMgr, query string, setting string) error {
	if !rst.PoolMgr().ConnectionActive(rst) {
		return nil
	}

	if err := rst.ProcessSimpleQuery(&pgproto3.Query{String: query}, true); err != nil {
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
		return rst.ProcessSimpleQuery(&pgproto3.Query{String: query}, true)
	}
	return nil
}

// TODO: unit tests
func (s *QueryStateExecutorImpl) ProcCopyPrepare(ctx context.Context, mgr meta.EntityMgr, stmt *lyx.Copy, attachedCopy bool) (*pgcopy.CopyState, error) {
	spqrlog.Zero.Debug().
		Uint("client", s.cl.ID()).
		Msg("client pre-process copy")

	var relname *rfqn.RelationFQN

	switch q := stmt.TableRef.(type) {
	case *lyx.RangeVar:
		relname = rfqn.RelationFQNFromRangeRangeVar(q)
	}
	// Read delimiter from COPY options
	delimiter := byte('\t')
	for _, opt := range stmt.Options {
		if opt == nil {
			/* ???? */
			continue
		}
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
	if attachedCopy {
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
		rr, err := mgr.GetReferenceRelation(ctx, relname)
		if err != nil {
			return nil, err
		}

		return &pgcopy.CopyState{
			Scatter:          true,
			ExecutionTargets: rr.ListStorageRoutes(),
		}, nil
	}

	dRel := ds.GetRelation(relname)

	hashFunc := make([]hashfunction.HashFunctionType, len(dRel.DistributionKey))

	krs, err := mgr.ListKeyRanges(ctx, ds.Id)
	if err != nil {
		return nil, err
	}

	co := make([]int, len(dRel.DistributionKey))
	for dKey := range dRel.DistributionKey {

		if v, err := hashfunction.HashFunctionByName(dRel.DistributionKey[dKey].HashFunction); err != nil {
			return nil, err
		} else {
			hashFunc[dKey] = v
		}

		colOffset := -1
		for indx, c := range stmt.Columns {
			if c == dRel.DistributionKey[dKey].Column {
				colOffset = indx
				break
			}
		}
		if colOffset == -1 {
			return nil, fmt.Errorf("failed to resolve target copy column offset")
		}
		co[dKey] = colOffset
	}

	return &pgcopy.CopyState{
		Delimiter:    delimiter,
		Krs:          krs,
		RM:           rmeta.NewRoutingMetadataContext(s.cl, mgr),
		Ds:           ds,
		HashFunc:     hashFunc,
		ColumnOffset: co,
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
		for _, et := range cps.ExecutionTargets {
			if err := s.cl.Server().SendShard(data, et); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}

	var leftoverMsgData []byte = nil

	rowsMp := map[string][]byte{}

	/* like hashfunction array */
	values := make([]any, len(cps.HashFunc))

	backMap := map[int]int{}

	for i, v := range cps.ColumnOffset {
		backMap[v] = i
	}

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

			if indx, ok := backMap[currentAttr]; ok {
				tmp, err := hashfunction.ApplyHashFunctionOnStringRepr(data.Data[prevDelimiter:i], cps.Ds.ColTypes[indx], cps.HashFunc[indx])
				if err != nil {
					return nil, err
				}
				values[indx] = tmp
			}

			currentAttr++
			prevDelimiter = i + 1
		}
		if b != '\n' {
			continue
		}

		/* By this time, row should contains all routing info */

		// check where this tuple should go
		currroute, err := cps.RM.DeparseKeyWithRangesInternal(ctx, values, cps.Krs)
		if err != nil {
			return nil, err
		}

		/* reset values  */
		values = make([]interface{}, len(cps.HashFunc))

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
func (s *QueryStateExecutorImpl) ProcCopyComplete(query pgproto3.FrontendMessage) (txstatus.TXStatus, error) {
	spqrlog.Zero.Debug().
		Uint("client", s.cl.ID()).
		Type("query-type", query).
		Msg("client process copy end")
	server := s.cl.Server()
	/* non-null server should never be set to null here until we call Unroute()
	in complete relay */
	if server == nil {
		return txstatus.TXERR, fmt.Errorf("client not routed in copy complete phase, resetting")
	}

	for _, sh := range server.Datashards() {
		if err := sh.Send(query); err != nil {
			return txstatus.TXERR, err
		}
	}

	var ccmsg *pgproto3.CommandComplete = nil
	var errmsg *pgproto3.ErrorResponse = nil

	txt := txstatus.TXIDLE

	for _, sh := range server.Datashards() {

	wl:
		for {
			msg, err := sh.Receive()
			if err != nil {
				return txt, err
			}
			switch v := msg.(type) {
			case *pgproto3.ReadyForQuery:
				/* should always be NOT idle */
				if v.TxStatus == byte(txstatus.TXIDLE) {
					return txt, fmt.Errorf("copy state out of sync")
				}
				if txt != txstatus.TXERR {
					txt = txstatus.TXStatus(v.TxStatus)
				}
				break wl
			case *pgproto3.CommandComplete:
				ccmsg = v
			case *pgproto3.ErrorResponse:
				errmsg = v
			default:
			}
		}
	}

	if errmsg != nil {
		if err := s.cl.Send(errmsg); err != nil {
			return txt, err
		}
	} else {
		if ccmsg == nil {
			return txt, fmt.Errorf("copy state out of sync")
		}
		if err := s.cl.Send(ccmsg); err != nil {
			return txt, err
		}
	}

	return txt, nil
}

func (s *QueryStateExecutorImpl) copyExecutor(mgr meta.EntityMgr, q *lyx.Copy, doFinalizeTx, attachedCopy bool) error {

	var leftoverMsgData []byte
	ctx := context.TODO()

	cps, err := s.ProcCopyPrepare(ctx, mgr, q, attachedCopy)
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
			if txt, err := s.ProcCopyComplete(cpMsg); err != nil {
				return err
			} else {
				if doFinalizeTx {
					if txt == txstatus.TXACT {
						return s.ExecCommitTx("COMMIT")
					}
					return s.ExecRollbackServer()
				}
			}

			return nil
		default:
			/* panic? */
		}
	}
}

// TODO : unit tests
func (s *QueryStateExecutorImpl) ProcQuery(qd *QueryDesc, mgr meta.EntityMgr, waitForResp bool, replyCl bool) ([]pgproto3.BackendMessage, error) {

	switch q := qd.P.(type) {
	case plan.VirtualPlan:
		/* execute logic without shard dispatch */

		/* XXX: fetch all tuples from sub-plan */

		if q.SubPlan == nil {

			/* only send row description for simple proto case */
			switch qd.Msg.(type) {
			case *pgproto3.Query:

				if err := s.Client().Send(&pgproto3.RowDescription{
					Fields: q.VirtualRowCols,
				}); err != nil {
					return nil, err
				}

				if err := s.Client().Send(&pgproto3.DataRow{
					Values: q.VirtualRowVals,
				}); err != nil {
					return nil, err
				}
				if err := s.Client().Send(&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				}); err != nil {
					return nil, err
				}
			case *pgproto3.Sync:

				if err := s.Client().Send(&pgproto3.DataRow{
					Values: q.VirtualRowVals,
				}); err != nil {
					return nil, err
				}
				if err := s.Client().Send(&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				}); err != nil {
					return nil, err
				}
			}

			return nil, nil
		} else {
			return nil, rerrors.ErrComplexQuery
		}
	}

	serv := s.Client().Server()

	if serv == nil {
		return nil, fmt.Errorf("client %p is out of transaction sync with router", s.Client())
	}

	doFinalizeTx := false
	attachedCopy := s.cl.ExecuteOn() != "" || s.TxStatus() == txstatus.TXACT

	switch qd.Stmt.(type) {
	case *lyx.Copy:
		spqrlog.Zero.Debug().Str("txstatus", serv.TxStatus().String()).Msg("prepared copy state")

		if serv.TxStatus() == txstatus.TXIDLE {
			if err := s.DeployTx(serv, "BEGIN"); err != nil {
				return nil, err
			}
			doFinalizeTx = true
		}
	}

	spqrlog.Zero.Debug().
		Uints("shards", shard.ShardIDs(serv.Datashards())).
		Type("query-type", qd.Msg).Type("plan-type", qd.P).
		Msg("relay process plan")

	if err := DispatchPlan(qd, serv, s.Client(), replyCl); err != nil {
		return nil, err
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
		msg, recvIndex, err := serv.Receive()
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

			return nil, s.copyExecutor(mgr, q, doFinalizeTx, attachedCopy)
		case *pgproto3.DataRow:
			spqrlog.Zero.Debug().
				Str("server", serv.Name()).
				Msg("received datarow message from server")
			if replyCl {
				switch v := qd.P.(type) {
				case plan.DataRowFilter:
					if v.FilterIndex == recvIndex {
						err = s.Client().Send(msg)
						if err != nil {
							return nil, err
						}
					}
				default:
					err = s.Client().Send(msg)
					if err != nil {
						return nil, err
					}
				}
			} else {
				unreplied = append(unreplied, msg)
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
