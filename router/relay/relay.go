package relay

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"slices"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/qdb"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/pg-sharding/spqr/router/qparser"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/pg-sharding/spqr/router/slice"
	"github.com/pg-sharding/spqr/router/statistics"
	"github.com/pg-sharding/spqr/router/virtual"
	"github.com/pg-sharding/spqr/router/xproto"
)

type RelayStateMgr interface {
	slice.ExecutionSliceMgr

	QueryExecutor() QueryStateExecutor
	QueryRouter() qrouter.QueryRouter
	PoolMgr() poolmgr.PoolMgr

	/* Cleanup is same as reset, but no tx management */
	Cleanup() error
	Reset() error
	ResetWithError(err error) error

	Parse(query string, doCaching bool) ([]lyx.Node, string, error)

	CompleteRelay() error
	CompleteRelayClient() error
	Close() error
	Client() client.RouterClient

	PrepareExecutionSlice(
		ctx context.Context,
		rm *rmeta.RoutingMetadataContext,
		prevPlan plan.Plan) (plan.Plan, error)

	PrepareRandomDispatchExecutionSlice(plan.Plan) (plan.Plan, func() error, error)
	PrepareTargetDispatchExecutionSlice(hintPlan plan.Plan) error

	/* process extended proto */
	ProcessSimpleQuery(q *pgproto3.Query, replyCl bool) error

	ProcessOneMsgCarefully(ctx context.Context, msg pgproto3.FrontendMessage) error

	PipelineCleanup()

	ProcQueryAdvancedTx(query string, binderQ func() error, doCaching bool) (*PortalDesc, error)
}

type PortalDesc struct {
	rd     *pgproto3.RowDescription
	nodata *pgproto3.NoData
}

type ParseCacheEntry struct {
	ps   []lyx.Node
	comm string
	stmt lyx.Node
}

type RelayStateImpl struct {
	routingDecisionPlan plan.Plan

	Qr      qrouter.QueryRouter
	qse     QueryStateExecutor
	qp      qparser.QParser
	plainQ  string
	Cl      client.RouterClient
	poolMgr poolmgr.PoolMgr

	msgBuf []pgproto3.FrontendMessage

	bindQueryPlan       plan.Plan
	bindQueryPlanMP     map[string]plan.Plan
	lastBindName        string
	unnamedPortalExists bool

	execute   func() error
	executeMp map[string]func() error

	saveBind        pgproto3.Bind
	saveBindNamed   map[string]*pgproto3.Bind
	savedPortalDesc map[string]*PortalDesc

	WaitSync bool

	parseCache map[string]ParseCacheEntry
	savedRM    map[string]*rmeta.RoutingMetadataContext

	// buffer of messages to process on Sync request
	xBuf []pgproto3.FrontendMessage
}

// SetTxStatus implements poolmgr.ConnectionKeeper.
func (rst *RelayStateImpl) SetTxStatus(status txstatus.TXStatus) {
	rst.qse.SetTxStatus(status)
}

// TxStatus implements poolmgr.ConnectionKeeper.
func (rst *RelayStateImpl) TxStatus() txstatus.TXStatus {
	return rst.qse.TxStatus()
}

func NewRelayState(qr qrouter.QueryRouter, client client.RouterClient, manager poolmgr.PoolMgr) RelayStateMgr {
	mgr := qr.Mgr()
	var d qdb.DCStateKeeper

	/* in case of local router, mgr can be nil */
	if mgr != nil {
		d = mgr.DCStateKeeper()
	}

	return &RelayStateImpl{
		msgBuf:              nil,
		qse:                 NewQueryStateExecutor(d, qr.Mgr(), manager, client),
		Qr:                  qr,
		Cl:                  client,
		poolMgr:             manager,
		execute:             nil,
		executeMp:           map[string]func() error{},
		saveBind:            pgproto3.Bind{},
		saveBindNamed:       map[string]*pgproto3.Bind{},
		bindQueryPlan:       nil,
		bindQueryPlanMP:     map[string]plan.Plan{},
		savedPortalDesc:     map[string]*PortalDesc{},
		parseCache:          map[string]ParseCacheEntry{},
		savedRM:             map[string]*rmeta.RoutingMetadataContext{},
		unnamedPortalExists: false,
		WaitSync:            false,
	}
}

func (rst *RelayStateImpl) QueryRouter() qrouter.QueryRouter {
	return rst.Qr
}

func (rst *RelayStateImpl) QueryExecutor() QueryStateExecutor {
	return rst.qse
}

func (rst *RelayStateImpl) PoolMgr() poolmgr.PoolMgr {
	return rst.poolMgr
}

func (rst *RelayStateImpl) Client() client.RouterClient {
	return rst.Cl
}

func (rst *RelayStateImpl) gangDeployPrepStmt(hash uint64, d *prepstatement.PreparedStatementDefinition) (*prepstatement.PreparedStatementDescriptor, pgproto3.BackendMessage, error) {
	serv := rst.Client().Server()

	if serv == nil {
		return nil, nil, server.ErrMultiShardSyncBroken
	}

	shards := serv.Datashards()
	if len(shards) == 0 {
		return nil, nil, spqrerror.New(spqrerror.SPQR_NO_DATASHARD, "No active shards")
	}

	var rd *prepstatement.PreparedStatementDescriptor
	var replyMsg pgproto3.BackendMessage

shardLoop:
	for i, shard := range shards {
		shardRd, shardReplyMsg, err := gangMemberDeployPreparedStatement(shard, hash, d)
		if err != nil {
			return nil, nil, err
		}
		if i == 0 {
			rd = shardRd
			replyMsg = shardReplyMsg
		}
		/* If prepared statement is not actually deployed by backend, return quickly */
		switch replyMsg.(type) {
		case *pgproto3.ErrorResponse:
			break shardLoop
		}
	}
	return rd, replyMsg, nil
}

func pstmtDoesNotExistsErr(name string) error {
	if len(name) > 0 {
		return spqrerror.Newf(spqrerror.PG_PREPARED_STATEMENT_DOES_NOT_EXISTS, "prepared statement \"%s\" does not exist", name)
	}

	return spqrerror.New(spqrerror.PG_PREPARED_STATEMENT_DOES_NOT_EXISTS, "unnamed prepared statement does not exist")
}

func (rst *RelayStateImpl) Close() error {
	_ = rst.Reset()

	return rst.Cl.Close()
}

func (rst *RelayStateImpl) Cleanup() error {
	err := poolmgr.UnrouteCommon(rst.Client(), rst.QueryExecutor().ActiveShards())

	if err != nil {
		spqrlog.Zero.Debug().Err(err).Msg("reset relay server err")
	}

	rst.QueryExecutor().ActiveShardsReset()
	rst.QueryExecutor().Reset()

	_ = rst.Client().Reset()

	return rst.Client().Unroute()
}

// TODO : unit tests
func (rst *RelayStateImpl) Reset() error {
	err := rst.Cleanup()

	/* See flush vs sync */
	rst.QueryExecutor().SetTxStatus(txstatus.TXIDLE)

	return err
}

var ErrMatchShardError = fmt.Errorf("failed to match datashard")

// TODO : unit tests
func (rst *RelayStateImpl) initExecutor(p plan.Plan) error {

	switch q := p.(type) {
	case *plan.VirtualPlan:
		if q.SubPlan == nil {
			return nil
		}
	}

	if err := rst.QueryExecutor().InitPlan(p); err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Uint("client", rst.Client().ID()).
			Msg("client encounter while initialing server connection")
		return err
	}

	/* if transaction is explicitly requested, deploy */
	return rst.QueryExecutor().DeploySliceTransactionBlock()
}

// TODO : unit tests
func (rst *RelayStateImpl) expandRoutes(routes []kr.ShardKey) error {
	// if there is no routes to expand, there is nowhere to do
	if len(routes) == 0 {
		return nil
	}

	serv := rst.Client().Server()

	if serv == nil || serv.TxStatus() == txstatus.TXERR {
		/* should never happen */
		return fmt.Errorf("unexpected server expand request")
	}

	_ = rst.Client().SwitchServerConn(rst.Client().Server().ToMultishard())

	if err := rst.QueryExecutor().ExpandRoutes(routes); err != nil {
		return err
	}

	/* take care of session param if we told to */
	/* TODO: fix */
	// if rst.Client().MaintainParams() {
	// 	query := rst.Cl.ConstructClientParams()
	// 	spqrlog.Zero.Debug().
	// 		Uint("client", rst.Client().ID()).
	// 		Str("query", query.String).
	// 		Msg("setting params for client")
	// 	_, err := rst.QueryExecutor().ExecuteSlice(query, rst.qp.Stmt(), rst.Qr.Mgr(), true, false)
	// 	return err
	// }

	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) CreateSlicedPlan(
	ctx context.Context,
	rm *rmeta.RoutingMetadataContext) (plan.Plan, error) {

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("drb", rst.Client().DefaultRouteBehaviour()).
		Str("exec_on", rst.Client().ExecuteOn()).
		Msg("create plan for current statement")

	var queryPlan plan.Plan

	if v := rst.Client().ExecuteOn(); v != "" {
		queryPlan = &plan.ShardDispatchPlan{
			PStmt: rst.qp.Stmt(),
			ExecTarget: kr.ShardKey{
				Name: v,
			},
		}
	} else {

		var err error

		queryPlan, err = rst.Qr.PlanQuery(ctx, rm)

		if err != nil {
			return nil, spqrerror.Newf(spqrerror.SPQR_COMPLEX_QUERY, "%w", err).Query(rst.plainQ)
		}

		/* XXX: fix this. This behaviour break regression tests */
		// if rst.Client().ShowNoticeMsg() {
		// 	if err := rst.Client().ReplyNotice(fmt.Sprintf("selected query plan %T %+v", queryPlan, queryPlan)); err != nil {
		// 		return nil, err
		// 	}
		// }
	}

	if rst.Client().Rule().PoolMode == config.PoolModeVirtual {
		/* never try to get connection */
		switch queryPlan.(type) {
		case *plan.VirtualPlan:
			return queryPlan, nil
		default:
			return nil, fmt.Errorf("query processing for this client is disabled")
		}
	}

	if rm != nil && rm.UsedSelectQueryAdjust && rst.Client().ShowNoticeMsg() {
		_ = rst.Client().ReplyNotice("query used select adjust for JOIN semantics")
	}

	if rm != nil && rm.AutoLinearize && rst.Client().ShowNoticeMsg() {
		_ = rst.Client().ReplyNotice("auto-linearize query dispatch because of hazard upsert")
	}

	switch v := queryPlan.(type) {
	case *plan.VirtualPlan, *plan.ScatterPlan, *plan.ShardDispatchPlan, *plan.DataRowFilter:
		return queryPlan, nil
	default:
		return nil, fmt.Errorf("unexpected query plan %T", v)
	}
}

// formatShardNoticeMessage formats a shard notice message using the configured template format
// Supported placeholders: {shard}, {host}, {hostname}, {port}, {user}, {db}, {pid}, {az}, {id}, {tx_status}, {tx_served}
func formatShardNoticeMessage(template string, shardInstance shard.ShardHostInstance) string {
	result := template

	// Extract hostname and port from host address
	host := shardInstance.InstanceHostname()
	hostname := host
	port := ""
	if colonIndex := strings.LastIndex(host, ":"); colonIndex != -1 {
		hostname = host[:colonIndex]
		port = host[colonIndex+1:]
	}

	// Replace all placeholders
	result = strings.ReplaceAll(result, "{shard}", shardInstance.SHKey().Name)
	result = strings.ReplaceAll(result, "{host}", host)
	result = strings.ReplaceAll(result, "{hostname}", hostname)
	result = strings.ReplaceAll(result, "{port}", port)
	result = strings.ReplaceAll(result, "{user}", shardInstance.Usr())
	result = strings.ReplaceAll(result, "{db}", shardInstance.DB())
	result = strings.ReplaceAll(result, "{pid}", fmt.Sprintf("%d", shardInstance.Pid()))
	result = strings.ReplaceAll(result, "{az}", shardInstance.Instance().AvailabilityZone())
	result = strings.ReplaceAll(result, "{id}", fmt.Sprintf("%d", shardInstance.ID()))
	result = strings.ReplaceAll(result, "{tx_status}", shardInstance.TxStatus().String())
	result = strings.ReplaceAll(result, "{tx_served}", fmt.Sprintf("%d", shardInstance.TxServed()))

	return result
}

// TODO : unit tests
func replyShardMatchesWithHosts(client client.RouterClient, serv server.Server, shardKeys []kr.ShardKey) error {
	// Create a map of shard key names to actual shard instances for quick lookup
	shardInstanceMap := make(map[string]shard.ShardHostInstance)
	for _, shardInstance := range serv.Datashards() {
		shardInstanceMap[shardInstance.SHKey().Name] = shardInstance
	}

	// Get the notice message format from config
	messageFormat := config.RouterConfig().NoticeMessageFormat

	// Build shard info with the configured format
	var shardInfos []string
	for _, shkey := range shardKeys {
		if shardInstance, exists := shardInstanceMap[shkey.Name]; exists {
			shardInfo := formatShardNoticeMessage(messageFormat, shardInstance)
			shardInfos = append(shardInfos, shardInfo)
		} else {
			// Fallback to just shard name if we can't find the instance
			shardInfos = append(shardInfos, shkey.Name)
		}
	}

	sort.Strings(shardInfos)
	shardMatches := strings.Join(shardInfos, ",")

	return client.ReplyNotice("send query to shard(s) : " + shardMatches)
}

func (rst *RelayStateImpl) CompleteRelayClient() error {
	return rst.Client().Send(rst.QueryExecutor().RFQ())
}

func (rst *RelayStateImpl) CompleteRelay() error {
	rst.unnamedPortalExists = false

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("txstatus", rst.qse.TxStatus().String()).
		Msg("complete relay iter")

	if err := rst.QueryExecutor().CompleteTx(rst.QueryExecutor()); err != nil {
		spqrlog.Zero.Error().
			Uint("client", rst.Client().ID()).
			Str("txstatus", rst.qse.TxStatus().String()).
			Err(err).
			Msg("failed to complete relay")

		if err := rst.Reset(); err != nil {
			return err
		}
		return err
	}

	rst.QueryExecutor().Reset()

	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) ResetWithError(err error) error {

	// XXX: use rst.QueryExecutor().FailStatement

	_ = rst.Client().ReplyErr(err)
	return rst.Reset()
}

// TODO : unit tests
func (rst *RelayStateImpl) AddExtendedProtocMessage(q pgproto3.FrontendMessage) {
	spqrlog.Zero.Debug().
		Interface("query", q).
		Msg("adding extended protocol message")
	rst.xBuf = append(rst.xBuf, q)
}

// TODO : unit tests
func (rst *RelayStateImpl) gangDeployPrepStmtByName(qname string) (*prepstatement.PreparedStatementDescriptor, pgproto3.BackendMessage, error) {

	def := rst.Client().PreparedStatementDefinitionByName(qname)
	hash := rst.Client().PreparedStatementQueryHashByName(qname)
	name := fmt.Sprintf("%d", hash)

	spqrlog.Zero.Debug().
		Str("name", qname).
		Str("query", def.Query).
		Uint64("hash", hash).
		Uint("client", rst.Client().ID()).
		Msg("deploy prepared statement")

	return rst.gangDeployPrepStmt(hash, &prepstatement.PreparedStatementDefinition{
		Name:          name,
		Query:         def.Query,
		ParameterOIDs: def.ParameterOIDs,
	})
}

var (
	pgexec   = &pgproto3.Execute{}
	pgsync   = &pgproto3.Sync{}
	pgNoData = &pgproto3.NoData{}

	portalClose = &pgproto3.Close{
		ObjectType: 'P',
	}

	emptyExecFunc = func() error {
		return nil
	}
)

func (rst *RelayStateImpl) relayParsePrepared(
	ctx context.Context,
	name, query string, parameterOIDs []uint32) (pgproto3.BackendMessage, error) {

	startTime := time.Now()

	// analyze statement and maybe rewrite query
	stmts, _, err := rst.Parse(query, true)
	if err != nil {
		return nil, err
	}

	/* XXX: check that we have reference relation insert here */
	stmt := stmts[0]

	rm, err := rst.Qr.AnalyzeQuery(ctx, rst.Cl, rst.Cl.Rule(), query, stmt)
	if err != nil {
		return nil, err
	}

	rst.savedRM[name] = rm

	def := &prepstatement.PreparedStatementDefinition{
		Name:          name,
		Query:         query,
		ParameterOIDs: parameterOIDs,
	}

	/* XXX: very stupid here - is query exactly like insert into ref_rel values()
	* or select __spqr__virtual_func()? ?*/
	switch parsed := stmt.(type) {
	case *lyx.Insert:
		switch parsed.SubSelect.(type) {
		case *lyx.ValueClause:

			switch rf := parsed.TableRef.(type) {
			case *lyx.RangeVar:
				qualName := rfqn.RelationFQNFromRangeRangeVar(rf)
				if ds, err := rst.Qr.Mgr().GetRelationDistribution(ctx, qualName); err != nil {
					return nil, err
				} else if ds.Id == distributions.REPLICATED {
					rel, err := rst.Qr.Mgr().GetReferenceRelation(ctx, qualName)
					if err != nil {
						return nil, err
					}

					if _, err := planner.InsertSequenceParamRef(ctx, query, rel.ColumnSequenceMapping, stmt, def); err != nil {
						return nil, err
					}
				}
				/* else distributed relation. */
			}
		}
	case *lyx.Select:
		if len(parsed.TargetList) == 1 {
			switch tle := parsed.TargetList[0].(type) {
			case *lyx.FuncApplication:
				if virtual.IsVirtualFuncName(tle.Name) {
					// ok
					rst.Client().StorePreparedStatement(def)

					if err := rst.Client().Send(&pgproto3.ParseComplete{}); err != nil {
						return nil, err
					}

					/* Do not deploy our virtual functions to postgres. */
					return nil, nil
				}
			}
		}
	}

	p, fin, err := rst.PrepareRandomDispatchExecutionSlice(rst.routingDecisionPlan)
	if err != nil {
		return nil, err
	}

	rst.routingDecisionPlan = p

	rst.Client().StorePreparedStatement(def)

	hash := rst.Client().PreparedStatementQueryHashByName(name)

	spqrlog.Zero.Debug().
		Str("name", name).
		Str("query", query).
		Uint64("hash", hash).
		Uint("client", rst.Client().ID()).
		Msg("Parsing prepared statement")

	if config.RouterConfig().PgprotoDebug {
		if err := rst.Client().ReplyDebugNoticef("name %v, query %v, hash %d", name, query, hash); err != nil {
			return nil, err
		}
	}

	/* TODO: refactor code to make this less ugly */
	saveTxStatus := rst.qse.TxStatus()

	_, retMsg, err := rst.gangDeployPrepStmtByName(name)
	if err != nil {
		return nil, err
	}

	rst.qse.SetTxStatus(saveTxStatus)

	// tdb: fix this
	rst.plainQ = query

	if err := fin(); err != nil {
		return nil, err
	}

	spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeParse, query, time.Since(startTime))
	return retMsg, nil
}

func (rst *RelayStateImpl) DescribePrepared(objType byte, name string, dMsg *pgproto3.Describe) error {
	// save txstatus because it may be overwritten if we have no backend connection
	saveTxStat := rst.qse.TxStatus()

	if objType == xproto.ObjectTypePortal {

		if !rst.unnamedPortalExists {
			return spqrerror.New(spqrerror.PG_PORTAL_DOES_NOT_EXISTS, "portal \"\" does not exist")
		}

		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Str("last-bind-name", rst.lastBindName).
			Msg("Describe portal")

		if portDesc, ok := rst.savedPortalDesc[rst.lastBindName]; ok {
			if portDesc.rd != nil {
				// send to the client
				if err := rst.Client().Send(portDesc.rd); err != nil {
					return err
				}
			}
			if portDesc.nodata != nil {
				// send to the client
				if err := rst.Client().Send(portDesc.nodata); err != nil {
					return err
				}
			}
		} else {

			p := rst.bindQueryPlan
			if name != "" {
				if _, ok := rst.executeMp[name]; !ok {
					return spqrerror.New(
						spqrerror.PG_PORTAL_DOES_NOT_EXISTS,
						fmt.Sprintf("portal \"%s\" does not exists", name))
				}
				p = rst.bindQueryPlanMP[name]
			}

			switch q := p.(type) {
			case *plan.VirtualPlan:
				// skip deploy

				// send to the client
				if err := rst.Client().Send(&pgproto3.RowDescription{
					Fields: q.TTS.Desc,
				}); err != nil {
					return err
				}

			default:
				/* SingleShard or random shard plans */

				err := rst.PrepareTargetDispatchExecutionSlice(p)
				if err != nil {
					return err
				}

				rst.routingDecisionPlan = p

				if _, _, err := rst.gangDeployPrepStmtByName(rst.lastBindName); err != nil {
					return err
				}

				var bnd *pgproto3.Bind

				if name == "" {
					bnd = &rst.saveBind
				} else {
					bnd = rst.saveBindNamed[name]
				}

				/* XXX: maybe optimize allocation here */

				if dMsg == nil {
					dMsg = &pgproto3.Describe{
						ObjectType: objType,
						Name:       name,
					}
				}

				cachedPd, err := sliceDescribePortal(rst.Client().Server(), dMsg, bnd)
				if err != nil {
					return err
				}
				if cachedPd.rd != nil {
					// send to the client
					if err := rst.Client().Send(cachedPd.rd); err != nil {
						return err
					}
				}
				if cachedPd.nodata != nil {
					// send to the client
					if err := rst.Client().Send(cachedPd.nodata); err != nil {
						return err
					}
				}

				rst.savedPortalDesc[rst.lastBindName] = cachedPd
			}
		}
	} else {

		/* q.ObjectType == 'S' */
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Str("stmt-name", name).
			Msg("Describe prep statement")

		def := rst.Client().PreparedStatementDefinitionByName(name)

		if def == nil {
			/* this prepared statement was not prepared by client */
			return pstmtDoesNotExistsErr(name)
		}

		p, fin, err := rst.PrepareRandomDispatchExecutionSlice(rst.routingDecisionPlan)
		if err != nil {
			return err
		}

		rst.routingDecisionPlan = p

		rd, _, err := rst.gangDeployPrepStmtByName(name)
		if err != nil {
			return err
		}

		desc := rd.ParamDesc
		if desc != nil {
			// if we did overwrite something - remove our
			// columns from output
			for ind := range def.OverwriteRemoveParamIDs {
				// NB: ind are zero - indexed
				desc.ParameterOIDs = slices.Delete(desc.ParameterOIDs, ind-1, ind)
			}

			if err := rst.Client().Send(desc); err != nil {
				return err
			}
		}

		if rd.NoData {
			if err := rst.Client().Send(pgNoData); err != nil {
				return err
			}
		} else {
			if rd.RowDesc != nil {
				if err := rst.Client().Send(rd.RowDesc); err != nil {
					return err
				}
			}
		}

		if err := fin(); err != nil {
			return err
		}
	}

	rst.qse.SetTxStatus(saveTxStat)
	return nil
}

func (rst *RelayStateImpl) BindPrepared(
	ctx context.Context,
	preparedStatement string,
	destinationPortal string,
	parameters [][]byte,
	parameterFormatCodes []int16,
	resultFormatCodes []int16,
) error {
	startTime := time.Now()

	spqrlog.Zero.Debug().
		Str("name", preparedStatement).
		Str("portal", destinationPortal).
		Uint("client", rst.Client().ID()).
		Msg("Binding prepared statement")

	// Here we are going to actually redirect the query to the execution shard.
	// However, to execute commit, rollbacks, etc., we need to wait for the next query
	// or process it locally (set statement)

	def := rst.Client().PreparedStatementDefinitionByName(preparedStatement)

	if def == nil {
		/* this prepared statement was not prepared by client */
		return pstmtDoesNotExistsErr(preparedStatement)
	}

	if def.OverwriteRemoveParamIDs != nil {
		// we did query overwrite for sole reason -
		// to insert next sequence value.
		// XXX: this needs a massive refactor later

		v, err := rst.Qr.IdRange().NextVal(ctx, def.SeqName)
		if err != nil {
			return err
		}

		parameters = append(parameters, fmt.Appendf(nil, "%d", v))
	}

	// We implicitly assume that there is always Execute after Bind for the same portal.
	// however, postgresql protocol allows some more cases.
	if err := rst.Client().ReplyBindComplete(); err != nil {
		return err
	}

	rst.lastBindName = preparedStatement
	rst.unnamedPortalExists = true

	/* only populate map for non-empty portal */
	if destinationPortal == "" {
		rst.execute = emptyExecFunc
	} else {
		rst.executeMp[destinationPortal] = emptyExecFunc
	}

	pd, err := rst.ProcQueryAdvancedTx(def.Query, func() error {
		var bnd *pgproto3.Bind

		if destinationPortal == "" {
			bnd = &rst.saveBind
		} else {
			rst.saveBindNamed[destinationPortal] = &pgproto3.Bind{}
			bnd = rst.saveBindNamed[destinationPortal]
		}

		bnd.DestinationPortal = destinationPortal

		rm := rst.savedRM[preparedStatement]

		hash := rst.Client().PreparedStatementQueryHashByName(preparedStatement)

		bnd.PreparedStatement = fmt.Sprintf("%d", hash)
		bnd.ParameterFormatCodes = parameterFormatCodes
		rst.Client().SetBindParams(parameters)
		rst.Client().SetParamFormatCodes(parameterFormatCodes)
		bnd.ResultFormatCodes = resultFormatCodes
		bnd.Parameters = parameters

		ctx := context.TODO()

		// Do not respond with BindComplete, as the relay step should take care of itself.
		queryPlan, err := rst.PrepareExecutionSlice(ctx, rm, rst.routingDecisionPlan)

		if err != nil {
			return err
		}

		rst.routingDecisionPlan = queryPlan

		if destinationPortal == "" {
			rst.bindQueryPlan = rst.routingDecisionPlan
		} else {
			rst.bindQueryPlanMP[destinationPortal] = rst.routingDecisionPlan
		}

		if rst.routingDecisionPlan == nil {
			return fmt.Errorf("extended xproto state out of sync")
		}

		f := func() error {
			p := rst.bindQueryPlan
			if destinationPortal != "" {
				p = rst.bindQueryPlanMP[destinationPortal]
			}
			forceSimple := false

			switch q := p.(type) {
			case *plan.ScatterPlan:
				forceSimple = len(q.OverwriteQuery) != 0 && len(bnd.Parameters) == 0
			default:
			}
			switch p.(type) {
			case *plan.VirtualPlan:
			default:
				err := rst.PrepareTargetDispatchExecutionSlice(p)
				if err != nil {
					return err
				}

				def := rst.Client().PreparedStatementDefinitionByName(preparedStatement)
				hash := rst.Client().PreparedStatementQueryHashByName(preparedStatement)
				name := fmt.Sprintf("%d", hash)

				_, _, err = rst.gangDeployPrepStmt(hash, &prepstatement.PreparedStatementDefinition{
					Name:          name,
					Query:         def.Query,
					ParameterOIDs: def.ParameterOIDs,
				})

				if err != nil {
					return err
				}
			}

			return BindAndReadSliceResult(rst, forceSimple, bnd, destinationPortal)
		}

		/* only populate map for non-empty portal */
		if destinationPortal == "" {
			rst.execute = f
		} else {
			rst.executeMp[destinationPortal] = f
		}

		spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeBind, def.Query, time.Since(startTime))

		return nil

	}, true /* cache parsing for prep statement */)

	if err != nil {
		return err
	}

	if pd != nil {
		rst.savedPortalDesc[preparedStatement] = pd
	}
	spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeBind, def.Query, time.Since(startTime))
	return nil
}

func (rst *RelayStateImpl) ExecutePortal(portal string) error {
	startTime := time.Now()
	q := rst.plainQ
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("portal", portal).
		Msg("Execute prepared statement, reset saved bind")

	var err error

	if portal == "" {
		/* NB: unnamed portals are quite different is a sence of that they are
		* auto-closed on new bind msgs
		* From PostgreSQL doc:
		* Named portals must be explicitly closed before
		* they can be redefined by another Bind message,
		* but this is not required for the unnamed portal. */
		if rst.execute == nil {
			return rerrors.ErrRelaySyncLost
		}
		err = rst.execute()
		rst.execute = nil
		rst.bindQueryPlan = nil
	} else {
		err = rst.executeMp[portal]()
		/* Note we do not delete from executeMP, this is intentional */
		rst.bindQueryPlanMP[portal] = nil
	}

	if rst.lastBindName == "" {
		delete(rst.savedPortalDesc, rst.lastBindName)
	}
	rst.lastBindName = ""
	if err != nil {
		return err
	}

	spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeBind, q, time.Since(startTime))
	return nil
}

func (rst *RelayStateImpl) PipelineCleanup() {
	rst.bindQueryPlan = nil
	rst.WaitSync = false
}

func (rst *RelayStateImpl) ProcessOneMsg(ctx context.Context, msg pgproto3.FrontendMessage) error {

	if rst.WaitSync {
		return nil
	}

	if rst.QueryExecutor().TxStatus() == txstatus.TXERR {
		return errAbortedTx
	}

	switch currentMsg := msg.(type) {
	case *pgproto3.Parse:
		if retMsg, err := rst.relayParsePrepared(ctx, currentMsg.Name, currentMsg.Query, currentMsg.ParameterOIDs); err != nil {
			return err
		} else if retMsg != nil {
			if err := rst.Client().Send(retMsg); err != nil {
				return err
			}
		}

		return nil
	case *pgproto3.Bind:
		return rst.BindPrepared(ctx,
			currentMsg.PreparedStatement,
			currentMsg.DestinationPortal,
			currentMsg.Parameters,
			currentMsg.ParameterFormatCodes,
			currentMsg.ResultFormatCodes)

	case *pgproto3.Describe:
		return rst.DescribePrepared(currentMsg.ObjectType, currentMsg.Name, currentMsg)
	case *pgproto3.Execute:
		if err := rst.ExecutePortal(currentMsg.Portal); err != nil {
			return err
		}

		/* Okay, respond with CommandComplete first. */
		return rst.QueryExecutor().DeriveCommandComplete()

	case *pgproto3.Close:
		/* Validate ObjectType */
		switch currentMsg.ObjectType {
		case 'P':
			/* Portal */
			if currentMsg.Name != "" {
				delete(rst.executeMp, currentMsg.Name)
			}
			if err := rst.Client().ReplyCloseComplete(); err != nil {
				return err
			}
		case 'S':
			/* Statement */

			rst.Client().ClosePreparedStatement(currentMsg.Name)
			if err := rst.Client().ReplyCloseComplete(); err != nil {
				return err
			}

		default:
			/* Send proper protocol error. */
			/* this prepared statement was not prepared by client */
			return spqrerror.Newf(spqrerror.PG_ERRCODE_PROTOCOL_VIOLATION, "invalid CLOSE message subtype %d", currentMsg.ObjectType)
		}

		return nil
	default:
		return fmt.Errorf("unexpected query type %v", msg)
	}
}

func (rst *RelayStateImpl) ProcessOneMsgCarefully(ctx context.Context, msg pgproto3.FrontendMessage) error {

	if rst.QueryExecutor().TxStatus() == txstatus.TXIDLE {
		/* XXX: support implicit tx semantics here */
		statistics.RecordStartTime(statistics.StatisticsTypeRouter, time.Now(), rst.Client())
	}

	if err := rst.ProcessOneMsg(ctx, msg); err != nil {
		rst.WaitSync = true
		return err
	}
	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) Parse(query string, doCaching bool) ([]lyx.Node, string, error) {
	if cache, ok := rst.parseCache[query]; ok {
		rst.qp.SetStmt(cache.stmt)
		rst.qp.SetOriginQuery(query)
		return cache.ps, cache.comm, nil
	}

	stmts, comm, err := rst.qp.Parse(query)

	if err != nil {
		return nil, "", err
	}

	/* XXX remove from here */
	switch stm := rst.qp.Stmt().(type) {
	case *lyx.Insert:
		// load columns from information schema
		if len(stm.Columns) == 0 {
			switch rv := stm.TableRef.(type) {
			case *lyx.RangeVar:
				cptr := rst.Qr.SchemaCache()
				if cptr != nil {
					var schemaErr error
					stm.Columns, schemaErr = cptr.GetColumns(rst.Cl.DB(), rfqn.RelationFQNFromFullName(rv.SchemaName, rv.RelationName))
					if schemaErr != nil {
						spqrlog.Zero.Err(schemaErr).Msg("get columns from schema cache")
						return stmts, comm, spqrerror.Newf(spqrerror.SPQR_FAILED_MATCH, "failed to get schema cache: %s", schemaErr)
					}
				}
			}
		}
	}

	if doCaching {
		stmt := rst.qp.Stmt()
		/* only cache specific type of queries */
		switch stmt.(type) {
		case *lyx.Select, *lyx.Insert, *lyx.Update, *lyx.Delete:
			rst.parseCache[query] = ParseCacheEntry{
				ps:   stmts,
				comm: comm,
				stmt: stmt,
			}
		}
	}

	rst.plainQ = query
	return stmts, comm, nil
}

var _ RelayStateMgr = &RelayStateImpl{}

// TODO : unit tests
func (rst *RelayStateImpl) PrepareExecutionSlice(ctx context.Context, rm *rmeta.RoutingMetadataContext, prevPlan plan.Plan) (plan.Plan, error) {

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Msg("preparing relay step for client")

	expandCurrentTx := false

	// txactive == 0 || activeSh == nil
	if !rst.poolMgr.ValidateGangChange(rst.QueryExecutor()) {
		if !rst.Client().EnhancedMultiShardProcessing() {

			/* TODO: fix this */
			prevPlan.SetStmt(rst.qp.Stmt())

			return prevPlan, nil
		}
		/* With engine v2 we can expand transaction on more targets */
		/* TODO: XXX */

		expandCurrentTx = true
	}

	q, err := rst.CreateSlicedPlan(ctx, rm)

	switch err {
	case nil:
		if expandCurrentTx {
			execTarg := q.ExecutionTargets()

			/*
			 * Try to keep single-shard connection as long as possible
			 */
			if len(execTarg) == 1 && len(rst.QueryExecutor().ActiveShards()) == 1 && rst.QueryExecutor().ActiveShards()[0].Name == execTarg[0].Name {
				return q, nil
			}

			/* else expand transaction */
			return q, rst.expandRoutes(execTarg)
		}

		if err := rst.initExecutor(q); err != nil {
			return nil, err
		}

		return q, nil
	case ErrMatchShardError:
		return nil, &spqrerror.SpqrError{
			Err:       err,
			ErrorCode: spqrerror.SPQR_NO_DATASHARD,
		}
	default:
		return q, err
	}
}

var noopCloseRouteFunc = func() error {
	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) PrepareTargetDispatchExecutionSlice(bindPlan plan.Plan) error {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Int("curr routes len", len(rst.QueryExecutor().ActiveShards())).
		Msg("preparing relay step for client on target route")

	// txactive == 0 || activeSh == nil
	// already has route, no need for any hint
	if !rst.poolMgr.ValidateGangChange(rst.QueryExecutor()) {
		return nil
	}

	if bindPlan == nil {
		return fmt.Errorf("failed to use hint route")
	}

	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")
	if len(rst.QueryExecutor().ActiveShards()) != 0 {
		if err := poolmgr.UnrouteCommon(rst.Client(), rst.QueryExecutor().ActiveShards()); err != nil {
			return err
		}
		rst.QueryExecutor().ActiveShardsReset()
	}

	err := rst.initExecutor(bindPlan)

	switch err {
	case nil:
		return nil
	case ErrMatchShardError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return err
	default:
		return err
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) PrepareRandomDispatchExecutionSlice(currentPlan plan.Plan) (plan.Plan, func() error, error) {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Msg("preparing relay step for client on any route")

	// txactive == 0 || activeSh == nil
	if !rst.poolMgr.ValidateGangChange(rst.QueryExecutor()) {
		return currentPlan, noopCloseRouteFunc, nil
	}

	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")

	cf := func() error {
		/* Active shards should be same as p.ExecutionTargets */
		err := poolmgr.UnrouteCommon(rst.Client(), rst.QueryExecutor().ActiveShards())
		rst.QueryExecutor().ActiveShardsReset()
		return err
	}

	/* No need to change gang, we can reuse existing gang. */
	if len(rst.QueryExecutor().ActiveShards()) == 1 {
		return currentPlan, cf, nil
	} else if len(rst.QueryExecutor().ActiveShards()) != 0 {
		if err := poolmgr.UnrouteCommon(rst.Client(), rst.QueryExecutor().ActiveShards()); err != nil {
			return nil, noopCloseRouteFunc, err
		}
		rst.QueryExecutor().ActiveShardsReset()
	}

	p, err := planner.SelectRandomDispatchPlan(rst.QueryRouter().DataShardsRoutes())
	if err != nil {
		return nil, noopCloseRouteFunc, err
	}

	err = rst.initExecutor(p)

	switch err {
	case nil:
		return p, cf, nil
	case ErrMatchShardError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return currentPlan, noopCloseRouteFunc, err
	default:
		return currentPlan, noopCloseRouteFunc, err
	}
}

func (rst *RelayStateImpl) ProcessSimpleQuery(q *pgproto3.Query, replyCl bool) error {
	ctx := context.TODO()

	rm, err := rst.Qr.AnalyzeQuery(ctx, rst.Cl, rst.Cl.Rule(), q.String, rst.qp.Stmt())
	if err != nil {
		return err
	}

	/* Now we can create plan for this statement */
	queryPlan, err := rst.PrepareExecutionSlice(ctx, rm, rst.routingDecisionPlan)

	if err != nil {
		/* some critical connection issue, client processing cannot be competed.
		* empty our msg buf */
		return err
	}

	rst.routingDecisionPlan = queryPlan

	es := &QueryDesc{
		Msg:    q,
		simple: true,
	}

	return rst.QueryExecutor().ExecuteSlice(
		es, queryPlan /*  ugh... fix this someday */, replyCl)
}
