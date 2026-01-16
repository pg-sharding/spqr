package relay

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/parser"
	"github.com/pg-sharding/spqr/router/pgcopy"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/pg-sharding/spqr/router/statistics"
	"github.com/pg-sharding/spqr/router/twopc"
	"golang.org/x/exp/slices"

	"github.com/pg-sharding/lyx/lyx"
)

type QueryStateExecutorImpl struct {
	txstatus.TxStatusMgr

	txStatus txstatus.TXStatus
	cl       client.RouterClient
	d        qdb.DCStateKeeper

	cacheCC pgproto3.CommandComplete
	cacheEQ pgproto3.EmptyQueryResponse

	poolMgr poolmgr.PoolMgr

	mgr meta.EntityMgr

	es ExecutorState
}

// ActiveShards implements [QueryStateExecutor].
func (s *QueryStateExecutorImpl) ActiveShards() []kr.ShardKey {
	return s.es.activeShards
}

// ActiveShardsReset implements [QueryStateExecutor].
func (s *QueryStateExecutorImpl) ActiveShardsReset() {
	s.es.activeShards = nil
}

// DataPending implements [QueryStateExecutor].
func (s *QueryStateExecutorImpl) DataPending() bool {
	server := s.Client().Server()

	if server == nil {
		return false
	}

	return server.DataPending()
}

// SyncCount implements [QueryStateExecutor].
func (s *QueryStateExecutorImpl) SyncCount() int64 {
	server := s.Client().Server()
	if server == nil {
		return 0
	}
	return server.Sync()
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

	/* TODO: deploy tx status on each gang. */

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

// InitPlan implements QueryStateExecutor.
func (s *QueryStateExecutorImpl) InitPlan(p plan.Plan) error {

	routes := p.ExecutionTargets()

	// if there is no routes configured, there is nowhere to route to
	if len(routes) == 0 {
		return ErrMatchShardError
	}

	if len(s.ActiveShards()) != 0 {
		spqrlog.Zero.Debug().
			Uint("relay state", spqrlog.GetPointer(s)).
			Int("len", len(s.ActiveShards())).
			Msg("unroute previous connections")

		if err := poolmgr.UnrouteCommon(s.Client(), s.ActiveShards()); err != nil {
			return err
		}
		s.es.activeShards = nil
		return fmt.Errorf("init plan called on active executor")
	}

	s.es.activeShards = routes

	if config.RouterConfig().PgprotoDebug {
		if err := s.Client().ReplyDebugNoticef("matched datashard routes %+v", routes); err != nil {
			return err
		}
	}

	var serv server.Server
	var err error

	/* Traverse and create gang for each slice. */

	if len(s.ActiveShards()) > 1 || p.Subplan() != nil {
		serv, err = server.NewMultiShardServer(s.Client().Route().MultiShardPool())
		if err != nil {
			return err
		}
	} else {
		serv = server.NewShardServer(s.Client().Route().MultiShardPool())
	}

	/* Assign top-level gang (output slice) to client */
	if err := s.Client().AssignServerConn(serv); err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Str("user", s.Client().Usr()).
		Str("db", s.Client().DB()).
		Uint("client", s.Client().ID()).
		Msg("allocate gang for client")

	for _, shkey := range s.ActiveShards() {
		spqrlog.Zero.Debug().
			Str("client tsa", string(s.Client().GetTsa())).
			Msg("adding shard with tsa")
		if err := s.Client().Server().AllocateGangMember(
			s.Client().ID(),
			shkey, s.Client().GetTsa()); err != nil {
			return err
		}
	}

	/* Do this in expand routes too. */
	if s.Client().MaintainParams() {
		query := s.Client().ConstructClientParams()
		spqrlog.Zero.Debug().
			Uint("client", s.Client().ID()).
			Str("query", query.String).
			Msg("setting params for client")

		qd := &QueryDesc{
			Msg:    query,
			simple: true,
		}

		if err := s.ExecuteSlice(qd, nil, false); err != nil {
			return err
		}
	}

	return nil
}

// DeploySliceTransactionBlock implements QueryStateExecutor.
func (s *QueryStateExecutorImpl) DeploySliceTransactionBlock() error {
	server := s.Client().Server()
	if server == nil {
		return errUnAttached
	}
	if s.txStatus == txstatus.TXIDLE {
		/* unexpected? */
		return nil
	}

	if !s.cl.EnhancedMultiShardProcessing() {
		if s.TxStatus() == txstatus.TXACT && len(server.Datashards()) > 1 {
			return fmt.Errorf("cannot route in an active transaction")
		}
	}

	return s.deployTxStatusInternal(server, s.es.savedBegin, txstatus.TXACT)
}

func (s *QueryStateExecutorImpl) DeploySliceTransactionQuery(query string) error {
	server := s.Client().Server()
	if server == nil {
		return errUnAttached
	}
	s.SetTxStatus(txstatus.TXACT)
	s.es.savedBegin = &pgproto3.Query{String: query}

	return s.deployTxStatusInternal(server, s.es.savedBegin, txstatus.TXACT)
}

func (s *QueryStateExecutorImpl) SetTxStatus(status txstatus.TXStatus) {
	s.txStatus = status
	/* handle implicit transactions - rollback all local state for params */
	s.cl.CleanupStatementSet()
}

func (s *QueryStateExecutorImpl) TxStatus() txstatus.TXStatus {
	return s.txStatus
}

func (s *QueryStateExecutorImpl) ExecBegin(query string, st *parser.ParseStateTXBegin) error {
	if s.poolMgr.ConnectionActive(s) {
		return s.DeploySliceTransactionQuery(query)
	}

	s.SetTxStatus(txstatus.TXACT)
	s.cl.StartTx()

	// explicitly set silent query message, as it can differ from query begin in xproto
	s.es.savedBegin = &pgproto3.Query{String: query}

	spqrlog.Zero.Debug().Uint("client", s.Client().ID()).Msg("start new transaction")

	for _, opt := range st.Options {
		switch opt {
		case lyx.TransactionReadOnly:
			s.Client().SetTsa(session.VirtualParamLevelLocal, config.TargetSessionAttrsPS)
		case lyx.TransactionReadWrite:
			s.Client().SetTsa(session.VirtualParamLevelLocal, config.TargetSessionAttrsRW)
		default:
			s.Client().SetTsa(session.VirtualParamLevelLocal, config.TargetSessionAttrsRW)
		}
	}
	return s.ReplyCommandComplete("BEGIN")
}

func (s *QueryStateExecutorImpl) ExecCommitTx(query string) error {
	spqrlog.Zero.Debug().Uint("client", s.cl.ID()).Str("commit strategy", s.cl.CommitStrategy()).Msg("execute commit")

	serv := s.cl.Server()

	if s.cl.CommitStrategy() == twopc.COMMIT_STRATEGY_2PC && len(serv.Datashards()) > 1 {
		if st, err := twopc.ExecuteTwoPhaseCommit(s.d, s.cl.ID(), serv); err != nil {
			return err
		} else {
			// serv.SetTxStatus(st)
			s.SetTxStatus(st)
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
func (s *QueryStateExecutorImpl) ExecCommit(query string) error {
	// Virtual tx case. Do the whole logic locally
	if !s.poolMgr.ConnectionActive(s) {
		s.cl.CommitActiveSet()
		_ = s.ReplyCommandComplete("COMMIT")
		s.SetTxStatus(txstatus.TXIDLE)
		return nil
	}

	if err := s.ExecCommitTx(query); err != nil {
		return err
	}

	s.Client().CommitActiveSet()
	return s.ReplyCommandComplete("COMMIT")
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
func (s *QueryStateExecutorImpl) ExecRollback(query string) error {
	// Virtual tx case. Do the whole logic locally
	if !s.poolMgr.ConnectionActive(s) {
		s.cl.Rollback()
		_ = s.ReplyCommandComplete("ROLLBACK")
		s.SetTxStatus(txstatus.TXIDLE)
		return nil
	}

	/* unroute will take care of tx server */
	if err := s.ExecRollbackServer(); err != nil {
		return err
	}
	s.cl.Rollback()
	return s.ReplyCommandComplete("ROLLBACK")
}

func (s *QueryStateExecutorImpl) ReplyCommandComplete(commandTag string) error {
	s.cacheCC.CommandTag = append([]byte(nil), commandTag...)
	s.es.cc = &s.cacheCC
	return nil
}

func (s *QueryStateExecutorImpl) ExecSet(rst RelayStateMgr, query string, name, value string) error {
	if len(name) == 0 {
		// some session characteristic, ignore
		return s.ReplyCommandComplete("SET")
	}
	if !s.poolMgr.ConnectionActive(s) {
		s.Client().SetParam(name, value)
		return s.ReplyCommandComplete("SET")
	}

	spqrlog.Zero.Debug().Str("name", name).Str("value", value).Msg("execute set query")
	if err := rst.ProcessSimpleQuery(&pgproto3.Query{String: query}, true); err != nil {
		return err
	}
	s.Client().SetParam(name, value)

	return nil
}

func (s *QueryStateExecutorImpl) ExecReset(rst RelayStateMgr, query, setting string) error {
	if rst.PoolMgr().ConnectionActive(rst.QueryExecutor()) {
		return rst.ProcessSimpleQuery(rst.Client().ConstructClientParams(), false)
	}
	return nil
}

func (s *QueryStateExecutorImpl) ExecResetMetadata(rst RelayStateMgr, query string, setting string) error {
	if !rst.PoolMgr().ConnectionActive(rst.QueryExecutor()) {
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

// TODO: unit tests
func (s *QueryStateExecutorImpl) ProcCopyPrepare(ctx context.Context, stmt *lyx.Copy, attachedCopy bool) (*pgcopy.CopyState, error) {
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
	ds, err := s.mgr.GetRelationDistribution(ctx, relname)
	if err != nil {
		return nil, err
	}
	if ds.Id == distributions.REPLICATED {
		rr, err := s.mgr.GetReferenceRelation(ctx, relname)
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

	krs, err := s.mgr.ListKeyRanges(ctx, ds.Id)
	if err != nil {
		return nil, err
	}

	schemaColMp := map[string]int{}
	ctm := map[string]string{}

	for dKey := range dRel.DistributionKey {
		if len(dRel.DistributionKey[dKey].Column) != 0 {
			ctm[dRel.DistributionKey[dKey].Column] = ds.ColTypes[dKey]
			schemaColMp[dRel.DistributionKey[dKey].Column] = dKey
		} else {
			for _, cr := range dRel.DistributionKey[dKey].Expr.ColRefs {
				ctm[cr.ColName] = cr.ColType
				schemaColMp[cr.ColName] = dKey
			}
		}

		if v, err := hashfunction.HashFunctionByName(dRel.DistributionKey[dKey].HashFunction); err != nil {
			return nil, err
		} else {
			hashFunc[dKey] = v
		}

	}

	for k := range schemaColMp {
		found := false
		for _, c := range stmt.Columns {
			if c == k {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("failed to resolve target copy column offset")
		}
	}

	return &pgcopy.CopyState{
		Delimiter:      delimiter,
		Krs:            krs,
		RM:             rmeta.NewRoutingMetadataContext(s.cl, "", nil /*XXX: fix this*/, nil, s.mgr),
		Ds:             ds,
		Drel:           dRel,
		HashFunc:       hashFunc,
		ColTypesMp:     ctm,
		SchemaColumns:  stmt.Columns,
		SchemaColumnMp: schemaColMp,
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
	routingTuple := make([]any, len(cps.Drel.DistributionKey))
	routingTupleItems := make([][][]byte, len(cps.Drel.DistributionKey))

	// Parse data
	// and decide where to route
	prevDelimiter := 0
	prevLine := 0
	attrIndx := 0

	for i, b := range data.Data {
		if i+2 < len(data.Data) && string(data.Data[i:i+2]) == "\\." {
			prevLine = len(data.Data)
			break
		}
		if b == '\n' || b == cps.Delimiter {
			if attrIndx >= len(cps.SchemaColumns) {
				return nil, fmt.Errorf("malformed data in copy")
			}
			if indx, ok := cps.SchemaColumnMp[cps.SchemaColumns[attrIndx]]; ok {
				routingTupleItems[indx] = append(routingTupleItems[indx], data.Data[prevDelimiter:i])
			}

			attrIndx++
			prevDelimiter = i + 1
		}
		if b != '\n' {
			continue
		}

		var err error

		/* By this time, row should contains all routing info */
		for i := range routingTupleItems {
			if len(routingTupleItems[i]) == 0 {
				return nil, fmt.Errorf("insufficient data in routing tuple")
			}

			if len(cps.Drel.DistributionKey[i].Column) != 0 {
				val, err := hashfunction.ApplyHashFunctionOnStringRepr(
					routingTupleItems[i][0],
					cps.Ds.ColTypes[i],
					cps.HashFunc[i])
				if err != nil {
					return nil, err
				}
				routingTuple[i] = val
			} else {
				if len(routingTupleItems[i]) != len(cps.Drel.DistributionKey[i].Expr.ColRefs) {
					return nil, fmt.Errorf("insufficient data in routing tuple")
				}

				hf := cps.HashFunc[i]

				acc := []byte{}
				for j, itemVal := range routingTupleItems[i] {

					lExpr, err := hashfunction.ApplyNonIdentHashFunctionOnStringRepr(itemVal,
						cps.Drel.DistributionKey[i].Expr.ColRefs[j].ColType, hf)

					if err != nil {
						spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
						return nil, err
					}

					acc = append(acc, hashfunction.EncodeUInt64(uint64(lExpr))...)
				}
				/* because we take hash of bytes */
				routingTuple[i], err = hashfunction.ApplyHashFunction(acc, qdb.ColumnTypeVarcharHashed, hf)

				if err != nil {
					spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
					return nil, err
				}
			}
		}

		// check where this tuple should go
		tuplePlan, err := cps.RM.DeparseKeyWithRangesInternal(ctx, routingTuple, cps.Krs)
		if err != nil {
			return nil, err
		}

		/* reset values  */
		routingTuple = make([]any, len(cps.Drel.DistributionKey))
		routingTupleItems = make([][][]byte, len(cps.Drel.DistributionKey))

		rowsMp[tuplePlan.Name] = append(rowsMp[tuplePlan.Name], data.Data[prevLine:i+1]...)
		attrIndx = 0
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
		s.es.eMsg = errmsg
	} else {
		if ccmsg == nil {
			return txt, fmt.Errorf("copy state out of sync")
		}

		s.cacheCC.CommandTag = append([]byte(nil), ccmsg.CommandTag...)

		s.es.cc = &s.cacheCC
	}

	return txt, nil
}

func (s *QueryStateExecutorImpl) copyFromExecutor() error {

	var leftoverMsgData []byte
	ctx := context.TODO()

	stmt := s.es.copyStmt
	if stmt == nil {
		return fmt.Errorf("failed to prepare copy context")
	}
	cs, ok := stmt.(*lyx.Copy)
	if !ok {
		return fmt.Errorf("failed to prepare copy context, not a copy statement")
	}
	cps, err := s.ProcCopyPrepare(ctx, cs, s.es.attachedCopy)
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
				if txt != s.cl.Server().TxStatus() {
					return rerrors.ErrExecutorSyncLost
				}
				s.SetTxStatus(txt)
			}

			return nil
		default:
			/* panic? */
		}
	}
}

// TODO : unit tests
func (s *QueryStateExecutorImpl) executeSlicePrepare(qd *QueryDesc, P plan.Plan, replyCl bool) error {

	s.Reset()
	/* XXX: refactor this into ExecutorReset */
	s.es.expectRowDesc = qd.simple

	switch P.(type) {
	case *plan.VirtualPlan:
		return nil
	default:
		serv := s.Client().Server()
		if serv == nil {
			return fmt.Errorf("client %p is out of transaction sync with router", s.Client())
		}

		s.es.attachedCopy = s.cl.ExecuteOn() != "" || s.TxStatus() == txstatus.TXACT

		/* Should be deploy plan (all slices) in implicit transaction block? */

		implicitTx := false

		if P != nil {

			stmt := P.Stmt()

			if stmt != nil {
				switch stmt.(type) {
				case *lyx.Copy:
					spqrlog.Zero.Debug().Str("txstatus", serv.TxStatus().String()).Msg("prepared copy state")

					s.es.copyStmt = stmt

					if serv.TxStatus() == txstatus.TXIDLE {
						implicitTx = true
					}
				}
			}
			if P.Subplan() != nil {
				if serv.TxStatus() == txstatus.TXIDLE {
					implicitTx = true
				}
			}
		}

		if implicitTx {
			s.es.doFinalizeTx = true
			return s.DeploySliceTransactionQuery("BEGIN")
		}

		return nil
	}
}

func (s *QueryStateExecutorImpl) executeInnerSlice(serv server.Server, p plan.Plan) error {

	/* The main difference between top-level and inner slices are that
	* inner slice results are not transferred to client.
	* Top-level slice outputs to client. */

	if sp := p.Subplan(); sp != nil {
		/* XXX: Do all required job in sub-plan */
		spqrlog.Zero.Debug().Uint("client-id", s.cl.ID()).Msg("executing sub plan")
		if err := s.executeInnerSlice(serv, sp); err != nil {
			return err
		}
	}

	if serv == nil {
		/* Malformed */
		return errUnAttached
	}

	/*  Inner slice should always contain per-shard overwrite message. */
	qd := &QueryDesc{
		simple: true,
	}

	spqrlog.Zero.Debug().Uint("client-id", s.cl.ID()).Msgf("dispatching slice plan: %+v", p)

	/* Before dispatching slice, expand server, if needed */

	if err := s.ExpandRoutes(p.ExecutionTargets()); err != nil {
		return err
	}

	/* Now dispatch this toplevel slice */
	if err := DispatchSlice(qd, p, s.Client(), true); err != nil {
		return err
	}

	return p.RunSlice(serv)
}

func (s *QueryStateExecutorImpl) executeSliceGuts(qd *QueryDesc, topPlan plan.Plan, replyCl bool) error {
	serv := s.Client().Server()

	if serv != nil {
		spqrlog.Zero.Debug().
			Uints("shards", shard.ShardIDs(serv.Datashards())).
			Type("query-type", qd.Msg).Type("plan-type", topPlan).
			Msg("toplevel plan process")
	} else {
		spqrlog.Zero.Debug().
			Type("query-type", qd.Msg).Type("plan-type", topPlan).
			Msg("toplevel plan process")
	}

	statistics.RecordStartTime(statistics.StatisticsTypeShard, time.Now(), s.Client())

	if topPlan != nil {
		if sp := topPlan.Subplan(); sp != nil {
			/* XXX: Do all required job in sub-plan */
			spqrlog.Zero.Debug().Uint("client-id", s.cl.ID()).Msg("executing sub plan")
			if err := s.executeInnerSlice(serv, sp); err != nil {
				return err
			}
		}
	}

	/* Prepare copy state, if needed. */
	if err := s.executeSlicePrepare(qd, topPlan, replyCl); err != nil {
		return err
	}

	switch q := topPlan.(type) {
	case *plan.VirtualPlan:
		/* execute logic without shard dispatch */

		if len(q.TTS.Raw) != 0 {
			/* only send row description for simple proto case */
			if s.es.expectRowDesc {
				if replyCl {
					if err := s.Client().Send(&pgproto3.RowDescription{
						Fields: q.TTS.Desc,
					}); err != nil {
						return err
					}
				}
			}

			for _, vals := range q.TTS.Raw {
				if err := s.Client().Send(&pgproto3.DataRow{
					Values: vals,
				}); err != nil {
					return err
				}
			}
		}

		if q.OverwriteCC != nil {
			s.es.cc = q.OverwriteCC
		} else {
			s.es.cc = &pgproto3.CommandComplete{
				CommandTag: fmt.Appendf(nil, "SELECT %d", len(q.TTS.Raw)),
			}
		}

		return nil

	case *plan.CopyPlan:

		if serv == nil {
			/* Malformed */
			return errUnAttached
		}

		/* Now dispatch this toplevel slice */
		if err := DispatchSlice(qd, topPlan, s.Client(), replyCl); err != nil {
			return err
		}

		msg, _, err := serv.Receive()
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Str("server", serv.Name()).
			Type("msg-type", msg).
			Msg("received message from server")

		switch msg.(type) {
		case *pgproto3.CopyInResponse:
			// handle replyCl somehow
			err = s.Client().Send(msg)
			if err != nil {
				return err
			}

			return s.copyFromExecutor()
		default:
			return server.ErrMultiShardSyncBroken
		}
	}

	if serv == nil {
		/* Malformed */
		return errUnAttached
	}

	/* Now dispatch this toplevel slice */
	if err := DispatchSlice(qd, topPlan, s.Client(), replyCl); err != nil {
		return err
	}

	for {
		msg, recvIndex, err := serv.Receive()
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Str("server", serv.Name()).
			Type("msg-type", msg).
			Msg("received message from server")

		switch v := msg.(type) {
		case *pgproto3.CopyInResponse:
			// handle replyCl somehow
			err = s.Client().Send(msg)
			if err != nil {
				return err
			}

			return s.copyFromExecutor()
		case *pgproto3.DataRow:
			if replyCl {
				switch v := topPlan.(type) {
				case *plan.DataRowFilter:
					if v.FilterIndex == recvIndex {
						err = s.Client().Send(msg)
						if err != nil {
							return err
						}
					}
				default:
					err = s.Client().Send(msg)
					if err != nil {
						return err
					}
				}
			}
		case *pgproto3.ReadyForQuery:
			s.SetTxStatus(txstatus.TXStatus(v.TxStatus))
			return nil
		case *pgproto3.ErrorResponse:

			if replyCl {
				s.es.eMsg = v
			}
		// never expect these msgs
		case *pgproto3.BindComplete:
			// skip
		case *pgproto3.ParseComplete, *pgproto3.CloseComplete:

			return rerrors.ErrExecutorSyncLost
		case *pgproto3.CommandComplete:
			/*
			* Safe for later reuse. For multi-slice statements
			* original CommandComplete may differ from any of
			* received from slices (including output slice). */
			if replyCl {
				s.cacheCC.CommandTag = append([]byte(nil), v.CommandTag...)

				s.es.cc = &s.cacheCC
			}
		case *pgproto3.RowDescription:
			if s.es.expectRowDesc {
				if replyCl {
					err = s.Client().Send(msg)
					if err != nil {
						return err
					}
				}
			} else {
				return fmt.Errorf("unexpected row description in slice deploy")
			}
		case *pgproto3.ParameterStatus:
			/* do not resent this to client */
		default:
			return fmt.Errorf("unexpected %T message type in executor slice deploy", msg)
		}
	}
}

// TODO : unit tests
func (s *QueryStateExecutorImpl) ExecuteSlice(qd *QueryDesc, topPlan plan.Plan, replyCl bool) error {

	if err := s.executeSliceGuts(qd, topPlan, replyCl); err != nil {
		return err
	}

	if s.es.doFinalizeTx {
		if s.TxStatus() == txstatus.TXACT {
			return s.ExecCommitTx("COMMIT")
		}
		return s.ExecRollbackServer()
	}

	return nil
}

func (s *QueryStateExecutorImpl) Client() client.RouterClient {
	return s.cl
}

func (s *QueryStateExecutorImpl) CompleteTx(mgr poolmgr.GangMgr) error {

	/* move this logic to executor */
	switch s.TxStatus() {
	case txstatus.TXIDLE:
		if err := s.Client().Send(&pgproto3.ReadyForQuery{
			TxStatus: byte(s.TxStatus()),
		}); err != nil {
			return err
		}

		if err := s.poolMgr.TXEndCB(mgr); err != nil {
			return err
		}

		statistics.RecordFinishedTransaction(time.Now(), s.Client())

		return nil
	case txstatus.TXERR:
		fallthrough
	case txstatus.TXACT:
		/* preserve same route. Do not unroute */
		return s.Client().Send(&pgproto3.ReadyForQuery{
			TxStatus: byte(s.TxStatus()),
		})
	default:
		return fmt.Errorf("unknown tx status %v", s.TxStatus())
	}
}

func (s *QueryStateExecutorImpl) ExpandRoutes(routes []kr.ShardKey) error {

	beforeTx := s.Client().Server().TxStatus()

	for _, shkey := range routes {
		if slices.ContainsFunc(s.ActiveShards(), func(c kr.ShardKey) bool {
			return shkey == c
		}) {
			continue
		}

		s.es.activeShards = append(s.es.activeShards, shkey)

		spqrlog.Zero.Debug().
			Str("client tsa", string(s.Client().GetTsa())).
			Str("deploying tx", beforeTx.String()).
			Msg("expanding shard with tsa")

		if err := s.Client().Server().ExpandGang(s.Client().ID(), shkey, s.Client().GetTsa(), beforeTx == txstatus.TXACT); err != nil {
			return err
		}
	}
	return nil
}

func (s *QueryStateExecutorImpl) DeriveCommandComplete() error {

	if s.es.replyEmptyQuery {
		return s.Client().Send(&s.cacheEQ)
	}

	/*
	* For single slice execution plans, we have two valid completion messages:
	* ErrorMessage and Command complete. If our output gang did not return either of them,
	* we are in big trouble */
	if s.es.cc == nil && s.es.eMsg == nil {
		return fmt.Errorf("failed to derive command complete for query")
	}
	if s.es.cc != nil {
		return s.Client().Send(s.es.cc)
	}

	return s.Client().Send(s.es.eMsg)
}

func (s *QueryStateExecutorImpl) ReplyEmptyQuery() {
	s.es.replyEmptyQuery = true
}

func (s *QueryStateExecutorImpl) FailStatement(err *pgproto3.ErrorResponse) {
	s.es.eMsg = err
}

func (s *QueryStateExecutorImpl) Reset() {
	s.es.expectRowDesc = false
	s.es.attachedCopy = false
	s.es.doFinalizeTx = false
	s.es.cc = nil
	s.es.eMsg = nil
	s.es.replyEmptyQuery = false
	s.es.copyStmt = nil
}

var _ QueryStateExecutor = &QueryStateExecutorImpl{}

func NewQueryStateExecutor(d qdb.DCStateKeeper, mgr meta.EntityMgr, poolMgr poolmgr.PoolMgr, cl client.RouterClient) QueryStateExecutor {
	return &QueryStateExecutorImpl{
		cl:       cl,
		d:        d,
		poolMgr:  poolMgr,
		mgr:      mgr,
		txStatus: txstatus.TXIDLE,
	}
}
