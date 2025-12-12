package relay

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/qdb"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/opentracing/opentracing-go"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/parser"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/pg-sharding/spqr/router/slice"
	"github.com/pg-sharding/spqr/router/statistics"
	"golang.org/x/exp/slices"
)

type RelayStateMgr interface {
	poolmgr.ConnectionKeeper
	slice.ExecutionSliceMgr

	QueryExecutor() QueryStateExecutor
	QueryRouter() qrouter.QueryRouter
	PoolMgr() poolmgr.PoolMgr

	Reset() error

	Parse(query string, doCaching bool) (parser.ParseState, string, error)

	CompleteRelay(replyCl bool) error
	Close() error
	Client() client.RouterClient

	PrepareExecutionSlice(
		ctx context.Context,
		rm *rmeta.RoutingMetadataContext,
		prevPlan plan.Plan) (plan.Plan, error)

	PrepareRandomDispatchExecutionSlice(plan.Plan) (plan.Plan, func() error, error)
	PrepareTargetDispatchExecutionSlice(hintPlan plan.Plan) error

	HoldRouting()
	UnholdRouting()

	/* process extended proto */
	ProcessSimpleQuery(q *pgproto3.Query, replyCl bool) error

	AddExtendedProtocMessage(q pgproto3.FrontendMessage)
	ProcessExtendedBuffer(ctx context.Context) error

	ProcQueryAdvancedTx(query string, binderQ func() error, doCaching, completeRelay bool) (*PortalDesc, error)
}

type PortalDesc struct {
	rd     *pgproto3.RowDescription
	nodata *pgproto3.NoData
}

type ParseCacheEntry struct {
	ps   parser.ParseState
	comm string
	stmt lyx.Node
}

type RelayStateImpl struct {
	activeShards []kr.ShardKey

	routingDecisionPlan plan.Plan

	Qr      qrouter.QueryRouter
	qse     QueryStateExecutor
	qp      parser.QParser
	plainQ  string
	Cl      client.RouterClient
	poolMgr poolmgr.PoolMgr

	msgBuf []pgproto3.FrontendMessage

	holdRouting bool

	bindQueryPlan       plan.Plan
	lastBindName        string
	unnamedPortalExists bool

	execute   func() error
	executeMp map[string]func() error

	saveBind        pgproto3.Bind
	savedPortalDesc map[string]*PortalDesc

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

// HoldRouting implements RelayStateMgr.
func (rst *RelayStateImpl) HoldRouting() {
	rst.holdRouting = true
}

// UnholdRouting implements RelayStateMgr.
func (rst *RelayStateImpl) UnholdRouting() {
	rst.holdRouting = false
}

func NewRelayState(qr qrouter.QueryRouter, client client.RouterClient, manager poolmgr.PoolMgr) RelayStateMgr {
	mgr := qr.Mgr()
	var d qdb.DCStateKeeper

	/* in case of local router, mgr can be nil */
	if mgr != nil {
		d = mgr.DCStateKeeper()
	}

	return &RelayStateImpl{
		activeShards:        nil,
		msgBuf:              nil,
		qse:                 NewQueryStateExecutor(d, client),
		Qr:                  qr,
		Cl:                  client,
		poolMgr:             manager,
		execute:             nil,
		executeMp:           map[string]func() error{},
		saveBind:            pgproto3.Bind{},
		savedPortalDesc:     map[string]*PortalDesc{},
		parseCache:          map[string]ParseCacheEntry{},
		savedRM:             map[string]*rmeta.RoutingMetadataContext{},
		unnamedPortalExists: false,
	}
}

func (rst *RelayStateImpl) SyncCount() int64 {
	server := rst.Client().Server()
	if server == nil {
		return 0
	}
	return server.Sync()
}

func (rst *RelayStateImpl) DataPending() bool {
	server := rst.Client().Server()

	if server == nil {
		return false
	}

	return server.DataPending()
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

func (rst *RelayStateImpl) Close() error {
	defer func() {
		if err := rst.Cl.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close client connection")
		}
	}()
	defer rst.ActiveShardsReset()
	return rst.poolMgr.UnRouteCB(rst.Cl, rst.activeShards)
}

func (rst *RelayStateImpl) ActiveShardsReset() {
	rst.activeShards = nil
}

func (rst *RelayStateImpl) ActiveShards() []kr.ShardKey {
	return rst.activeShards
}

// TODO : unit tests
func (rst *RelayStateImpl) Reset() error {
	rst.activeShards = nil
	rst.qse.SetTxStatus(txstatus.TXIDLE)

	_ = rst.Cl.Reset()

	return rst.Cl.Unroute()
}

var ErrSkipQuery = fmt.Errorf("wait for a next query")
var ErrMatchShardError = fmt.Errorf("failed to match datashard")

// TODO : unit tests
func (rst *RelayStateImpl) procRoutes(routes []kr.ShardKey) error {
	// if there is no routes configured, there is nowhere to route to
	if len(routes) == 0 {
		return ErrMatchShardError
	}

	spqrlog.Zero.Debug().
		Uint("relay state", spqrlog.GetPointer(rst)).
		Msg("unroute previous connections")

	if err := rst.Unroute(rst.activeShards); err != nil {
		return err
	}

	rst.activeShards = routes

	if config.RouterConfig().PgprotoDebug {
		if err := rst.Cl.ReplyDebugNoticef("matched datashard routes %+v", routes); err != nil {
			return err
		}
	}

	if err := rst.Connect(); err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Uint("client", rst.Client().ID()).
			Msg("client encounter while initialing server connection")
		return err
	}

	/* if transaction is explicitly requested, deploy */
	if err := rst.QueryExecutor().DeploySliceTransactionBlock(rst.Client().Server()); err != nil {
		return err
	}

	/* take care of session param if we told to */
	if rst.Client().MaintainParams() {
		query := rst.Cl.ConstructClientParams()
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Str("query", query.String).
			Msg("setting params for client")

		es := &ExecutorState{
			Msg: query,
			P:   nil,
		}

		replyClient := false

		if err := rst.qse.ExecuteSlicePrepare(es, rst.Qr.Mgr(), replyClient, true); err != nil {
			return err
		}

		return rst.qse.ExecuteSlice(es, rst.Qr.Mgr(), replyClient)
	}

	return nil
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

	beforeTx := rst.Client().Server().TxStatus()

	for _, shkey := range routes {
		if slices.ContainsFunc(rst.activeShards, func(c kr.ShardKey) bool {
			return shkey == c
		}) {
			continue
		}

		rst.activeShards = append(rst.activeShards, shkey)

		spqrlog.Zero.Debug().
			Str("client tsa", string(rst.Client().GetTsa())).
			Str("deploying tx", beforeTx.String()).
			Msg("expanding shard with tsa")

		if err := rst.Client().Server().ExpandDataShard(rst.Client().ID(), shkey, rst.Client().GetTsa(), beforeTx == txstatus.TXACT); err != nil {
			return err
		}
	}

	/* take care of session param if we told to */
	/* TODO: fix */
	// if rst.Client().MaintainParams() {
	// 	query := rst.Cl.ConstructClientParams()
	// 	spqrlog.Zero.Debug().
	// 		Uint("client", rst.Client().ID()).
	// 		Str("query", query.String).
	// 		Msg("setting params for client")
	// 	_, err := rst.qse.ExecuteSlice(query, rst.qp.Stmt(), rst.Qr.Mgr(), true, false)
	// 	return err
	// }

	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) CreateSlicedPlan(ctx context.Context, rm *rmeta.RoutingMetadataContext) (plan.Plan, error) {
	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")

	if config.RouterConfig().WithJaeger {
		span := opentracing.StartSpan("reroute")
		defer span.Finish()
		span.SetTag("user", rst.Cl.Usr())
		span.SetTag("db", rst.Cl.DB())
	}

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("drb", rst.Client().DefaultRouteBehaviour()).
		Str("exec_on", rst.Client().ExecuteOn()).
		Msg("rerouting the client connection, resolving shard")

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
			return nil, fmt.Errorf("error processing query '%v': %v", rst.plainQ, err)
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

// TODO : unit tests
func (rst *RelayStateImpl) Connect() error {
	var serv server.Server
	var err error

	if len(rst.ActiveShards()) > 1 {
		serv, err = server.NewMultiShardServer(rst.Cl.Route().ServPool())
		if err != nil {
			return err
		}
	} else {
		_ = rst.Cl.ReplyDebugNotice("open a connection to the single data shard")
		serv = server.NewShardServer(rst.Cl.Route().ServPool())
	}

	if err := rst.Cl.AssignServerConn(serv); err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Str("user", rst.Cl.Usr()).
		Str("db", rst.Cl.DB()).
		Uint("client", rst.Client().ID()).
		Msg("connect client to datashard routes")

	for _, shkey := range rst.ActiveShards() {
		spqrlog.Zero.Debug().
			Str("client tsa", string(rst.Client().GetTsa())).
			Msg("adding shard with tsa")
		if err := rst.Client().Server().AddDataShard(rst.Client().ID(), shkey, rst.Client().GetTsa()); err != nil {
			return err
		}
	}

	return nil
}

func (rst *RelayStateImpl) CompleteRelay(replyCl bool) error {
	rst.unnamedPortalExists = false

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("txstatus", rst.qse.TxStatus().String()).
		Msg("complete relay iter")

	/* move this logic to executor */
	switch rst.qse.TxStatus() {
	case txstatus.TXIDLE:
		if replyCl {
			if err := rst.Cl.Send(&pgproto3.ReadyForQuery{
				TxStatus: byte(rst.qse.TxStatus()),
			}); err != nil {
				return err
			}
		}

		if err := rst.poolMgr.TXEndCB(rst); err != nil {
			return err
		}

		statistics.RecordFinishedTransaction(time.Now(), rst.Client())

		return nil
	case txstatus.TXERR:
		fallthrough
	case txstatus.TXACT:
		if replyCl {
			if err := rst.Cl.Send(&pgproto3.ReadyForQuery{
				TxStatus: byte(rst.qse.TxStatus()),
			}); err != nil {
				return err
			}
		}
		/* preserve same route. Do not unroute */
		return nil
	default:
		_ = rst.Unroute(rst.activeShards)
		return fmt.Errorf("unknown tx status %v", rst.qse.TxStatus())
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) Unroute(shkey []kr.ShardKey) error {
	newActiveShards := make([]kr.ShardKey, 0)
	for _, el := range rst.activeShards {
		if slices.IndexFunc(shkey, func(k kr.ShardKey) bool {
			return k == el
		}) == -1 {
			newActiveShards = append(newActiveShards, el)
		}
	}
	if err := rst.poolMgr.UnRouteCB(rst.Cl, shkey); err != nil {
		return err
	}
	if len(newActiveShards) > 0 {
		rst.activeShards = newActiveShards
	} else {
		rst.activeShards = nil
	}

	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) UnRouteWithError(shkey []kr.ShardKey, errmsg error) error {
	_ = rst.poolMgr.UnRouteWithError(rst.Cl, shkey, errmsg)
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

// TODO : unit tests
func (rst *RelayStateImpl) ProcessExtendedBuffer(ctx context.Context) error {

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Int("xBuf", len(rst.xBuf)).
		Msg("process extended buffer")

	defer func() {
		// cleanup
		rst.xBuf = nil
		rst.bindQueryPlan = nil
	}()

	holdRoute := true

	anyPrepStmt := ""
	for _, msg := range rst.xBuf {
		switch q := msg.(type) {
		case *pgproto3.Bind:
			if anyPrepStmt == "" {
				anyPrepStmt = q.PreparedStatement
			} else if anyPrepStmt != q.PreparedStatement {
				holdRoute = false
			}
		}
	}

	if holdRoute {
		defer rst.UnholdRouting()
	}

	for _, msg := range rst.xBuf {

		switch currentMsg := msg.(type) {
		case *pgproto3.Parse:
			startTime := time.Now()

			// analyze statement and maybe rewrite query

			query := currentMsg.Query
			_, _, err := rst.Parse(query, true)
			if err != nil {
				return err
			}

			/* XXX: check that we have reference relation insert here */
			stmt := rst.qp.Stmt()

			rm, err := rst.Qr.AnalyzeQuery(ctx, rst.Cl, query, stmt)
			if err != nil {
				return err
			}

			rst.savedRM[currentMsg.Name] = rm

			def := &prepstatement.PreparedStatementDefinition{
				Name:          currentMsg.Name,
				Query:         query,
				ParameterOIDs: currentMsg.ParameterOIDs,
			}

			/* XXX: very stupid here - is query exactly like insert into ref_rel values()?*/
			switch parsed := stmt.(type) {
			case *lyx.Insert:
				switch parsed.SubSelect.(type) {
				case *lyx.ValueClause:

					switch rf := parsed.TableRef.(type) {
					case *lyx.RangeVar:
						qualName := rfqn.RelationFQNFromRangeRangeVar(rf)
						if ds, err := rst.Qr.Mgr().GetRelationDistribution(ctx, qualName); err != nil {
							return err
						} else if ds.Id == distributions.REPLICATED {
							rel, err := rst.Qr.Mgr().GetReferenceRelation(ctx, qualName)
							if err != nil {
								return err
							}

							if _, err := planner.InsertSequenceParamRef(ctx, query, rel.ColumnSequenceMapping, stmt, def); err != nil {
								return err
							}
						}
					}
				}
			}

			p, fin, err := rst.PrepareRandomDispatchExecutionSlice(rst.routingDecisionPlan)
			if err != nil {
				return err
			}

			rst.routingDecisionPlan = p

			rst.Client().StorePreparedStatement(def)

			hash := rst.Client().PreparedStatementQueryHashByName(currentMsg.Name)

			spqrlog.Zero.Debug().
				Str("name", currentMsg.Name).
				Str("query", currentMsg.Query).
				Uint64("hash", hash).
				Uint("client", rst.Client().ID()).
				Msg("Parsing prepared statement")

			if config.RouterConfig().PgprotoDebug {
				if err := rst.Client().ReplyDebugNoticef("name %v, query %v, hash %d", currentMsg.Name, currentMsg.Query, hash); err != nil {
					return err
				}
			}

			/* TODO: refactor code to make this less ugly */
			saveTxStatus := rst.qse.TxStatus()

			_, retMsg, err := rst.gangDeployPrepStmtByName(currentMsg.Name)
			if err != nil {
				return err
			}

			rst.qse.SetTxStatus(saveTxStatus)

			// tdb: fix this
			rst.plainQ = currentMsg.Query

			if err := rst.Client().Send(retMsg); err != nil {
				return err
			}

			if err := fin(); err != nil {
				return err
			}

			spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeParse, currentMsg.Query, time.Since(startTime))
		case *pgproto3.Bind:
			startTime := time.Now()

			spqrlog.Zero.Debug().
				Str("name", currentMsg.PreparedStatement).
				Str("portal", currentMsg.DestinationPortal).
				Uint("client", rst.Client().ID()).
				Msg("Binding prepared statement")

			// Here we are going to actually redirect the query to the execution shard.
			// However, to execute commit, rollbacks, etc., we need to wait for the next query
			// or process it locally (set statement)

			def := rst.Client().PreparedStatementDefinitionByName(currentMsg.PreparedStatement)

			if def == nil {
				/* this prepared statement was not prepared by client */
				return spqrerror.Newf(spqrerror.PG_PREPARED_STATEMENT_DOES_NOT_EXISTS, "prepared statement \"%s\" does not exist", currentMsg.PreparedStatement)
			}

			if def.OverwriteRemoveParamIds != nil {
				// we did query overwrite for sole reason -
				// to insert next sequence value.
				// XXX: this needs a massive refactor later

				v, err := rst.Qr.IdRange().NextVal(ctx, def.SeqName)
				if err != nil {
					return err
				}

				currentMsg.Parameters = append(currentMsg.Parameters, fmt.Appendf(nil, "%d", v))
			}

			// We implicitly assume that there is always Execute after Bind for the same portal.
			// however, postgresql protocol allows some more cases.
			if err := rst.Client().ReplyBindComplete(); err != nil {
				return err
			}

			rst.lastBindName = currentMsg.PreparedStatement
			rst.unnamedPortalExists = true

			/* only populate map for non-empty portal */
			if currentMsg.DestinationPortal == "" {
				rst.execute = emptyExecFunc
			} else {
				rst.executeMp[currentMsg.DestinationPortal] = emptyExecFunc
			}

			pd, err := rst.ProcQueryAdvancedTx(def.Query, func() error {
				rst.saveBind.DestinationPortal = currentMsg.DestinationPortal

				rm := rst.savedRM[currentMsg.PreparedStatement]

				hash := rst.Client().PreparedStatementQueryHashByName(currentMsg.PreparedStatement)

				rst.saveBind.PreparedStatement = fmt.Sprintf("%d", hash)
				rst.saveBind.ParameterFormatCodes = currentMsg.ParameterFormatCodes
				rst.Client().SetBindParams(currentMsg.Parameters)
				rst.Client().SetParamFormatCodes(currentMsg.ParameterFormatCodes)
				rst.saveBind.ResultFormatCodes = currentMsg.ResultFormatCodes
				rst.saveBind.Parameters = currentMsg.Parameters

				ctx := context.TODO()

				if rst.poolMgr.ValidateSliceChange(rst) || rst.Client().EnhancedMultiShardProcessing() {

					// Do not respond with BindComplete, as the relay step should take care of itself.
					queryPlan, err := rst.PrepareExecutionSlice(ctx, rm, rst.routingDecisionPlan)

					if err != nil {
						return err
					}

					rst.routingDecisionPlan = queryPlan
				}

				rst.bindQueryPlan = rst.routingDecisionPlan

				// hold route if appropriate

				if holdRoute {
					rst.HoldRouting()
				}

				if rst.bindQueryPlan == nil {
					return fmt.Errorf("extended xproto state out of sync")
				}

				switch rst.bindQueryPlan.(type) {
				case *plan.VirtualPlan:

					f := func() error {
						return BindAndReadSliceResult(rst, &rst.saveBind /* XXX: virtual query always empty portal? */, "")
					}

					/* only populate map for non-empty portal */
					if currentMsg.DestinationPortal == "" {
						rst.execute = f
					} else {
						rst.executeMp[currentMsg.DestinationPortal] = f
					}

					spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeBind, def.Query, time.Since(startTime))
					return nil
				default:
					f := func() error {

						err := rst.PrepareTargetDispatchExecutionSlice(rst.bindQueryPlan)
						if err != nil {
							return err
						}

						def := rst.Client().PreparedStatementDefinitionByName(currentMsg.PreparedStatement)
						hash := rst.Client().PreparedStatementQueryHashByName(currentMsg.PreparedStatement)
						name := fmt.Sprintf("%d", hash)

						_, _, err = rst.gangDeployPrepStmt(hash, &prepstatement.PreparedStatementDefinition{
							Name:          name,
							Query:         def.Query,
							ParameterOIDs: def.ParameterOIDs,
						})

						if err != nil {
							return err
						}

						return BindAndReadSliceResult(rst, &rst.saveBind, currentMsg.DestinationPortal)
					}

					/* only populate map for non-empty portal */
					if currentMsg.DestinationPortal == "" {
						rst.execute = f
					} else {
						rst.executeMp[currentMsg.DestinationPortal] = f
					}

					spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeBind, def.Query, time.Since(startTime))

					return nil
				}

			}, true /* cache parsing for prep statement */, false /* do not completeRelay*/)

			if err != nil {
				return err
			}

			if pd != nil {
				rst.savedPortalDesc[currentMsg.PreparedStatement] = pd
			}
			spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeBind, def.Query, time.Since(startTime))

		case *pgproto3.Describe:
			// save txstatus because it may be overwritten if we have no backend connection
			saveTxStat := rst.qse.TxStatus()

			if currentMsg.ObjectType == 'P' {

				if !rst.unnamedPortalExists {
					return spqrerror.New(spqrerror.PG_PORTAl_DOES_NOT_EXISTS, "portal \"\" does not exist")
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

					switch q := rst.bindQueryPlan.(type) {
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

						err := rst.PrepareTargetDispatchExecutionSlice(rst.bindQueryPlan)
						if err != nil {
							return err
						}

						rst.routingDecisionPlan = rst.bindQueryPlan

						if _, _, err := rst.gangDeployPrepStmtByName(rst.lastBindName); err != nil {
							return err
						}

						cachedPd, err := sliceDescribePortal(rst.Client().Server(), currentMsg, &rst.saveBind)
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
					Str("stmt-name", currentMsg.Name).
					Msg("Describe prep statement")

				def := rst.Client().PreparedStatementDefinitionByName(currentMsg.Name)

				if def == nil {
					/* this prepared statement was not prepared by client */
					return spqrerror.Newf(spqrerror.PG_PREPARED_STATEMENT_DOES_NOT_EXISTS, "prepared statement \"%s\" does not exist", currentMsg.Name)
				}

				p, fin, err := rst.PrepareRandomDispatchExecutionSlice(rst.routingDecisionPlan)
				if err != nil {
					return err
				}

				rst.routingDecisionPlan = p

				rd, _, err := rst.gangDeployPrepStmtByName(currentMsg.Name)
				if err != nil {
					return err
				}

				desc := rd.ParamDesc
				if desc != nil {
					// if we did overwrite something - remove our
					// columns from output
					for ind := range def.OverwriteRemoveParamIds {
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

		case *pgproto3.Execute:
			startTime := time.Now()
			q := rst.plainQ
			spqrlog.Zero.Debug().
				Uint("client", rst.Client().ID()).
				Str("portal", currentMsg.Portal).
				Msg("Execute prepared statement, reset saved bind")

			var err error

			if currentMsg.Portal == "" {
				/* NB: unnamed portals are quite different is a sence of that they are
				* auto-closed on new bind msgs
				* From PostgreSQL doc:
				* Named portals must be explicitly closed before
				* they can be redefined by another Bind message,
				* but this is not required for the unnamed portal. */
				err = rst.execute()
				rst.execute = nil
				rst.bindQueryPlan = nil
			} else {
				err = rst.executeMp[currentMsg.Portal]()
				/* Note we do not delete from exeuteMP, this is intentional */
				rst.bindQueryPlan = nil
			}

			if rst.lastBindName == "" {
				delete(rst.savedPortalDesc, rst.lastBindName)
			}
			rst.lastBindName = ""
			if err != nil {
				return err
			}
			spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeBind, q, time.Since(startTime))
		case *pgproto3.Close:
			/*  */
			if currentMsg.ObjectType == 'P' {
				if currentMsg.Name != "" {
					delete(rst.executeMp, currentMsg.Name)
				}
			}
		default:
			panic(fmt.Sprintf("unexpected query type %v", msg))
		}
	}

	return rst.CompleteRelay(true)
}

// TODO : unit tests
func (rst *RelayStateImpl) Parse(query string, doCaching bool) (parser.ParseState, string, error) {
	if cache, ok := rst.parseCache[query]; ok {
		rst.qp.SetStmt(cache.stmt)
		rst.qp.SetOriginQuery(query)
		return cache.ps, cache.comm, nil
	}

	state, comm, err := rst.qp.Parse(query)

	if err != nil {
		return nil, "", err
	}

	switch stm := rst.qp.Stmt().(type) {
	case *lyx.Insert:
		// load columns from information schema
		if len(stm.Columns) == 0 {
			switch tableref := stm.TableRef.(type) {
			case *lyx.RangeVar:
				cptr := rst.Qr.SchemaCache()
				if cptr != nil {
					var schemaErr error
					stm.Columns, schemaErr = cptr.GetColumns(rst.Cl.DB(), tableref.SchemaName, tableref.RelationName)
					if schemaErr != nil {
						spqrlog.Zero.Err(schemaErr).Msg("get columns from schema cache")
						return state, comm, spqrerror.Newf(spqrerror.SPQR_FAILED_MATCH, "failed to get schema cache: %s", err)
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
				ps:   state,
				comm: comm,
				stmt: stmt,
			}
		}
	}

	rst.plainQ = query
	return state, comm, nil
}

var _ RelayStateMgr = &RelayStateImpl{}

// TODO : unit tests
func (rst *RelayStateImpl) PrepareExecutionSlice(ctx context.Context, rm *rmeta.RoutingMetadataContext, prevPlan plan.Plan) (plan.Plan, error) {

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Msg("preparing relay step for client")

	if rst.holdRouting {
		return nil, nil
	}

	// txactive == 0 || activeSh == nil
	if !rst.poolMgr.ValidateSliceChange(rst) {
		if rst.Client().EnhancedMultiShardProcessing() {
			/* With engine v2 we can expand transaction on more targets */
			/* TODO: XXX */

			q, err := rst.CreateSlicedPlan(ctx, rm)
			if err != nil {
				return nil, err
			}
			execTarg := q.ExecutionTargets()

			/*
			 * Try to keep single-shard connection as long as possible
			 */
			if len(execTarg) == 1 && len(rst.ActiveShards()) == 1 && rst.ActiveShards()[0].Name == execTarg[0].Name {
				return q, nil
			}

			/* else expand transaction */
			return q, rst.expandRoutes(execTarg)
		}

		/* TODO: fix this */
		prevPlan.SetStmt(rst.qp.Stmt())

		return prevPlan, nil
	}

	q, err := rst.CreateSlicedPlan(ctx, rm)

	switch err {
	case nil:
		switch q.(type) {
		case *plan.VirtualPlan:
			return q, nil
		default:
			return q, rst.procRoutes(q.ExecutionTargets())
		}
	case ErrSkipQuery:
		if err := rst.Client().ReplyErr(err); err != nil {
			return nil, err
		}
		return nil, ErrSkipQuery
	case ErrMatchShardError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return nil, ErrSkipQuery
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
		Int("curr routes len", len(rst.activeShards)).
		Msg("preparing relay step for client on target route")

	if rst.holdRouting {
		return nil
	}

	// txactive == 0 || activeSh == nil
	// already has route, no need for any hint
	if !rst.poolMgr.ValidateSliceChange(rst) {
		return nil
	}

	if bindPlan == nil {
		return fmt.Errorf("failed to use hint route")
	}

	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")

	if config.RouterConfig().WithJaeger {
		span := opentracing.StartSpan("reroute")
		defer span.Finish()
		span.SetTag("user", rst.Cl.Usr())
		span.SetTag("db", rst.Cl.DB())
	}

	err := rst.procRoutes(bindPlan.ExecutionTargets())

	switch err {
	case nil:
		return nil
	case ErrSkipQuery:
		if err := rst.Client().ReplyErr(err); err != nil {
			return err
		}
		return ErrSkipQuery
	case ErrMatchShardError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return ErrSkipQuery
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

	if rst.holdRouting {
		return currentPlan, noopCloseRouteFunc, nil
	}

	// txactive == 0 || activeSh == nil
	if !rst.poolMgr.ValidateSliceChange(rst) {
		return currentPlan, noopCloseRouteFunc, nil
	}

	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")

	if config.RouterConfig().WithJaeger {
		span := opentracing.StartSpan("reroute")
		defer span.Finish()
		span.SetTag("user", rst.Cl.Usr())
		span.SetTag("db", rst.Cl.DB())
	}

	p, err := planner.SelectRandomDispatchPlan(rst.QueryRouter().DataShardsRoutes())
	if err != nil {
		return nil, noopCloseRouteFunc, err
	}
	err = rst.procRoutes(p.ExecutionTargets())

	switch err {
	case nil:
		return p, func() error {
			return rst.Unroute(p.ExecutionTargets())
		}, nil
	case ErrSkipQuery:
		if err := rst.Client().ReplyErr(err); err != nil {
			return currentPlan, noopCloseRouteFunc, err
		}
		return currentPlan, noopCloseRouteFunc, ErrSkipQuery
	case ErrMatchShardError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return currentPlan, noopCloseRouteFunc, ErrSkipQuery
	default:
		return currentPlan, noopCloseRouteFunc, err
	}
}

func (rst *RelayStateImpl) ProcessSimpleQuery(q *pgproto3.Query, replyCl bool) error {

	ctx := context.TODO()
	if rst.poolMgr.ValidateSliceChange(rst) || rst.Client().EnhancedMultiShardProcessing() {
		rm, err := rst.Qr.AnalyzeQuery(ctx, rst.Cl, q.String, rst.qp.Stmt())
		if err != nil {
			return err
		}

		// Do not respond with BindComplete, as the relay step should take care of itself.
		queryPlan, err := rst.PrepareExecutionSlice(ctx, rm, rst.routingDecisionPlan)

		if err != nil {
			/* some critical connection issue, client processing cannot be competed.
			* empty our msg buf */
			return err
		}

		rst.routingDecisionPlan = queryPlan
	}

	es := &ExecutorState{
		Msg: q,
		P:   rst.routingDecisionPlan, /*  ugh... fix this someday */
	}
	/* FIX this */
	es.P.SetStmt(rst.qp.Stmt())

	if err := rst.qse.ExecuteSlicePrepare(
		es, rst.Qr.Mgr(), replyCl, true); err != nil {
		return err
	}

	return rst.qse.ExecuteSlice(
		es, rst.Qr.Mgr(), replyCl)
}
