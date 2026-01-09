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
	"golang.org/x/exp/slices"
)

type RelayStateMgr interface {
	slice.ExecutionSliceMgr

	QueryExecutor() QueryStateExecutor
	QueryRouter() qrouter.QueryRouter
	PoolMgr() poolmgr.PoolMgr

	Reset() error
	ResetWithError(err error) error

	Parse(query string, doCaching bool) (parser.ParseState, string, error)

	CompleteRelay() error
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
	bindQueryPlanMP     map[string]plan.Plan
	lastBindName        string
	unnamedPortalExists bool

	execute   func() error
	executeMp map[string]func() error

	saveBind        pgproto3.Bind
	saveBindNamed   map[string]*pgproto3.Bind
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
		msgBuf:              nil,
		qse:                 NewQueryStateExecutor(d, manager, client),
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
	_ = rst.Reset()

	return rst.Cl.Close()
}

// TODO : unit tests
func (rst *RelayStateImpl) Reset() error {

	if err := poolmgr.UnrouteCommon(rst.Client(), rst.QueryExecutor().ActiveShards()); err != nil {
		return err
	}

	rst.QueryExecutor().ActiveShardsReset()
	rst.QueryExecutor().Reset()

	rst.QueryExecutor().SetTxStatus(txstatus.TXIDLE)

	_ = rst.Client().Reset()

	return rst.Client().Unroute()
}

var ErrMatchShardError = fmt.Errorf("failed to match datashard")

// TODO : unit tests
func (rst *RelayStateImpl) initExecutor(p plan.Plan) error {

	switch p.(type) {
	case *plan.VirtualPlan:
		return nil
	}

	if err := rst.QueryExecutor().InitPlan(p, rst.Qr.Mgr()); err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Uint("client", rst.Client().ID()).
			Msg("client encounter while initialing server connection")
		return err
	}

	/* if transaction is explicitly requested, deploy */
	if err := rst.QueryExecutor().DeploySliceTransactionBlock(); err != nil {
		return err
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
func (rst *RelayStateImpl) CreateSlicedPlan(ctx context.Context, rm *rmeta.RoutingMetadataContext) (plan.Plan, error) {
	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")

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

// TODO : unit tests
// If we enter this function, then we need to process whole messages buffer
// in current statement pipeline bounds.
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
				var bnd *pgproto3.Bind

				if currentMsg.DestinationPortal == "" {
					bnd = &rst.saveBind
				} else {
					rst.saveBindNamed[currentMsg.DestinationPortal] = &pgproto3.Bind{}
					bnd = rst.saveBindNamed[currentMsg.DestinationPortal]
				}

				bnd.DestinationPortal = currentMsg.DestinationPortal

				rm := rst.savedRM[currentMsg.PreparedStatement]

				hash := rst.Client().PreparedStatementQueryHashByName(currentMsg.PreparedStatement)

				bnd.PreparedStatement = fmt.Sprintf("%d", hash)
				bnd.ParameterFormatCodes = currentMsg.ParameterFormatCodes
				rst.Client().SetBindParams(currentMsg.Parameters)
				rst.Client().SetParamFormatCodes(currentMsg.ParameterFormatCodes)
				bnd.ResultFormatCodes = currentMsg.ResultFormatCodes
				bnd.Parameters = currentMsg.Parameters

				ctx := context.TODO()

				// Do not respond with BindComplete, as the relay step should take care of itself.
				queryPlan, err := rst.PrepareExecutionSlice(ctx, rm, rst.routingDecisionPlan)

				if err != nil {
					return err
				}

				rst.routingDecisionPlan = queryPlan

				if currentMsg.DestinationPortal == "" {
					rst.bindQueryPlan = rst.routingDecisionPlan
				} else {
					rst.bindQueryPlanMP[currentMsg.DestinationPortal] = rst.routingDecisionPlan
				}

				// hold route if appropriate

				if holdRoute {
					rst.HoldRouting()
				}

				if rst.routingDecisionPlan == nil {
					return fmt.Errorf("extended xproto state out of sync")
				}

				switch rst.routingDecisionPlan.(type) {
				case *plan.VirtualPlan:

					f := func() error {
						return BindAndReadSliceResult(rst, bnd /* XXX: virtual query always empty portal? */, "")
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

						p := rst.bindQueryPlan
						if currentMsg.DestinationPortal != "" {
							p = rst.bindQueryPlanMP[currentMsg.DestinationPortal]
						}

						err := rst.PrepareTargetDispatchExecutionSlice(p)
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

						return BindAndReadSliceResult(rst, bnd, currentMsg.DestinationPortal)
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

					p := rst.bindQueryPlan
					if currentMsg.Name != "" {
						p = rst.bindQueryPlanMP[currentMsg.Name]
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

						if currentMsg.Name == "" {
							bnd = &rst.saveBind
						} else {
							bnd = rst.saveBindNamed[currentMsg.Name]
						}

						cachedPd, err := sliceDescribePortal(rst.Client().Server(), currentMsg, bnd)
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
				/* Note we do not delete from executeMP, this is intentional */
				rst.bindQueryPlanMP[currentMsg.Portal] = nil
			}

			if rst.lastBindName == "" {
				delete(rst.savedPortalDesc, rst.lastBindName)
			}
			rst.lastBindName = ""
			if err != nil {
				return err
			}

			/* Okay, respond with CommandComplete first. */
			if err := rst.QueryExecutor().DeriveCommandComplete(); err != nil {
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

	return rst.CompleteRelay()
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
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return nil, err
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

	if rst.holdRouting {
		return nil
	}

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

	if rst.holdRouting {
		return currentPlan, noopCloseRouteFunc, nil
	}

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

	es := &QueryDesc{
		Msg: q,
		P:   rst.routingDecisionPlan, /*  ugh... fix this someday */
	}

	if err := rst.QueryExecutor().ExecuteSlicePrepare(
		es, rst.Qr.Mgr(), replyCl, true); err != nil {
		return err
	}

	return rst.QueryExecutor().ExecuteSlice(
		es, rst.Qr.Mgr(), replyCl)
}
