package qrouter

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/txstatus"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/server"

	"github.com/pg-sharding/lyx/lyx"
)

func (qr *ProxyQrouter) planFromNode(ctx context.Context, rm *rmeta.RoutingMetadataContext, node lyx.FromClauseNode) (plan.Plan, error) {
	spqrlog.Zero.Debug().
		Type("node-type", node).
		Msg("planning from node")

	var p plan.Plan = nil

	switch q := node.(type) {
	case *lyx.RangeVar:

		/* XXX: nothing, everything checked during analyze stage */

	case *lyx.JoinExpr:
		if tmp, err := qr.planFromNode(ctx, rm, q.Rarg); err != nil {
			return nil, err
		} else {
			p = plan.Combine(p, tmp)
		}
		if tmp, err := qr.planFromNode(ctx, rm, q.Larg); err != nil {
			return nil, err
		} else {
			p = plan.Combine(p, tmp)
		}

	case *lyx.SubSelect:
		return qr.planQueryV1(ctx, rm, q.Arg)
	default:
		// other cases to consider
		// lateral join, natural, etc
	}

	return p, nil
}
func (qr *ProxyQrouter) planFromClauseList(
	ctx context.Context,
	rm *rmeta.RoutingMetadataContext, clause []lyx.FromClauseNode) (plan.Plan, error) {

	var p plan.Plan = nil

	for _, node := range clause {
		tmp, err := qr.planFromNode(ctx, rm, node)
		if err != nil {
			return nil, err
		}
		p = plan.Combine(p, tmp)
	}

	return p, nil
}

// TODO : unit tests
// May return nil routing state here - thats ok
func (qr *ProxyQrouter) planQueryV1(
	ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
	qstmt lyx.Node) (plan.Plan, error) {

	if qstmt == nil {
		return nil, nil
	}

	switch stmt := qstmt.(type) {
	case *lyx.Select:

		var p plan.Plan

		/* We cannot route SQL statements without a FROM clause. However, there are a few cases to consider. */
		if len(stmt.FromClause) == 0 && (stmt.LArg == nil || stmt.RArg == nil) && stmt.WithClause == nil {
			var err error

			p, err = planner.PlanTargetList(ctx, rm, qr, stmt)
			if err != nil {
				return nil, err
			}
		}
		/*
		 * Then try to route  both branches
		 */

		if tmp, err := qr.planQueryV1(ctx, rm, stmt.LArg); err != nil {
			return nil, err
		} else {
			p = plan.Combine(p, tmp)
		}

		if tmp, err := qr.planQueryV1(ctx, rm, stmt.RArg); err != nil {
			return nil, err
		} else {
			p = plan.Combine(p, tmp)
		}

		tmp, err := planner.PlanWithClause(ctx, rm, qr, stmt.WithClause)
		if err != nil {
			return nil, err
		}

		p = plan.Combine(p, tmp)

		// collect table alias names, if any
		// for single-table queries, process as usual
		if tmp, err := qr.planFromClauseList(ctx, rm, stmt.FromClause); err != nil {
			return nil, err
		} else {
			p = plan.Combine(p, tmp)
		}

		return p, nil

	case *lyx.Insert:

		p, err := planner.PlanWithClause(ctx, rm, qr, stmt.WithClause)
		if err != nil {
			return nil, err
		}
		selectStmt := stmt.SubSelect
		if selectStmt == nil {
			return p, nil
		}

		var routingList [][]lyx.Node

		switch subS := selectStmt.(type) {
		case *lyx.Select:
			spqrlog.Zero.Debug().Msg("routing insert stmt on select clause")

			p, _ = qr.planQueryV1(ctx, rm, subS)

			/* try target list */
			spqrlog.Zero.Debug().Msgf("routing insert stmt on target list:%T", p)
			/* this target list for some insert (...) sharding column */

			routingList = [][]lyx.Node{subS.TargetList}
			/* record all values from tl */

			switch rf := stmt.TableRef.(type) {
			case *lyx.RangeVar:

				qualName := rfqn.RelationFQNFromRangeRangeVar(rf)

				if rs, err := rm.IsReferenceRelation(ctx, rf); err != nil {
					return nil, err
				} else if rs {
					rel, err := rm.Mgr.GetReferenceRelation(ctx, qualName)
					if err != nil {
						return nil, err
					}
					if len(rel.ColumnSequenceMapping) == 0 {
						// ok
						if p == nil {
							return &plan.ScatterPlan{
								ExecTargets: rel.ListStorageRoutes(),
							}, nil
						}

						// XXX: todo - check that sub select is not doing anything insane
						switch p.(type) {
						case *plan.VirtualPlan, *plan.ScatterPlan, *plan.RandomDispatchPlan:
							if stmt.Returning != nil {
								return &plan.DataRowFilter{
									SubPlan: &plan.ScatterPlan{
										ExecTargets: rel.ListStorageRoutes(),
									},
									FilterIndex: 0,
								}, nil
							}
							return &plan.ScatterPlan{
								ExecTargets: rel.ListStorageRoutes(),
							}, nil
						default:
							return nil, rerrors.ErrComplexQuery
						}
					}
					return nil, rerrors.ErrComplexQuery
				} else {
					shs, err := planner.PlanDistributedRelationInsert(ctx, routingList, rm, stmt)
					if err != nil {
						return nil, err
					}
					for _, sh := range shs {
						if sh.Name != shs[0].Name {
							return nil, rerrors.ErrComplexQuery
						}
					}
					if len(shs) > 0 {
						p = plan.Combine(p, &plan.ShardDispatchPlan{
							ExecTarget:         shs[0],
							TargetSessionAttrs: config.TargetSessionAttrsRW,
						})
					}
					return p, nil
				}
			default:
				return nil, rerrors.ErrComplexQuery
			}

		case *lyx.ValueClause:
			/* record all values from values scan */
			routingList = subS.Values

			switch rf := stmt.TableRef.(type) {
			case *lyx.RangeVar:
				if rs, err := rm.IsReferenceRelation(ctx, rf); err != nil {
					return nil, err
				} else if rs {
					/* If reference relation, use planner v2 */
					p, err := planner.PlanReferenceRelationInsertValues(ctx, rm, stmt.Columns, rf, subS, qr.idRangeCache)

					if err != nil {
						return nil, err
					}
					if stmt.Returning != nil {
						return &plan.DataRowFilter{
							SubPlan:     p,
							FilterIndex: 0,
						}, nil
					}
					return p, nil
				} else {
					shs, err := planner.PlanDistributedRelationInsert(ctx, routingList, rm, stmt)
					if err != nil {
						return nil, err
					}
					/* XXX: give change for engine v2 to rewrite queries */
					for _, sh := range shs {
						if sh.Name != shs[0].Name {
							/* try to rewrite, but only for simple protocol */
							if len(rm.ParamRefs) == 0 {
								return planner.RewriteDistributedRelBatchInsert(rm.Query, shs)
							}
							return nil, rerrors.ErrComplexQuery
						}
					}

					if len(shs) > 0 {
						p = plan.Combine(p, &plan.ShardDispatchPlan{
							ExecTarget:         shs[0],
							TargetSessionAttrs: config.TargetSessionAttrsRW,
						})
					}
					return p, nil
				}
			default:
				return nil, rerrors.ErrComplexQuery
			}

		default:
			return p, nil
		}

	case *lyx.Update:

		p, err := planner.PlanWithClause(ctx, rm, qr, stmt.WithClause)
		if err != nil {
			return nil, err
		}

		clause := stmt.Where
		if clause == nil {
			return nil, nil
		}

		switch q := stmt.TableRef.(type) {
		case *lyx.RangeVar:

			rqdn := rfqn.RelationFQNFromRangeRangeVar(q)

			if d, err := rm.GetRelationDistribution(ctx, rqdn); err != nil {
				return nil, err
			} else if d.Id == distributions.REPLICATED {
				if rm.SPH.EnhancedMultiShardProcessing() {

					plr := planner.PlannerV2{}

					tmp, err := plr.PlanDistributedQuery(ctx, rm, stmt, true)
					if err != nil {
						return nil, err
					}
					p = plan.Combine(p, tmp)
					return p, nil
				}
				return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
			}
		default:
			return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
		}

		return p, nil
	case *lyx.Delete:

		p, err := planner.PlanWithClause(ctx, rm, qr, stmt.WithClause)
		if err != nil {
			return nil, err
		}

		clause := stmt.Where
		if clause == nil {
			return nil, nil
		}

		switch q := stmt.TableRef.(type) {
		case *lyx.RangeVar:

			rqdn := rfqn.RelationFQNFromRangeRangeVar(q)

			if d, err := rm.GetRelationDistribution(ctx, rqdn); err != nil {
				return nil, err
			} else if d.Id == distributions.REPLICATED {
				if rm.SPH.EnhancedMultiShardProcessing() {
					plr := planner.PlannerV2{}

					tmp, err := plr.PlanDistributedQuery(ctx, rm, stmt, true)
					if err != nil {
						return nil, err
					}
					p = plan.Combine(p, tmp)
					return p, nil
				}
				return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
			}
		default:
			return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
		}

		return p, nil
	}

	return nil, nil
}

// Returns state, is read-only flag and err if any
func (qr *ProxyQrouter) RouteWithRules(ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
	stmt lyx.Node) (plan.Plan, error) {
	if stmt == nil {
		// empty statement
		return &plan.RandomDispatchPlan{}, nil
	}

	/*
	* Currently, deparse only first query from multi-statement query msg (Enhance)
	 */

	/* TODO: delay this until step 2. */

	var pl plan.Plan
	pl = nil

	/*
	 * Step 1: traverse query tree and deparse mapping from
	 * columns to their values (either constant or expression).
	 * Note that exact (routing) value of (sharding) column may not be
	 * known after this phase, as it can be Parse Step of Extended proto.
	 */

	switch qs := stmt.(type) {

	/* TDB: comments? */
	case *lyx.Insert:

		rs, err := qr.planQueryV1(ctx, rm, stmt)
		if err != nil {
			return nil, err
		}

		pl = plan.Combine(pl, rs)

	case *lyx.Select:

		/*
		 *  Sometimes we have problems with some cases. For example, if a client
		 *  tries to access information schema AND other relation in same TX.
		 *  We are unable to serve this properly.
		 *  But if this is a catalog-only query, we can route it to any shard.
		 */
		hasInfSchema, onlyCatalog, anyCatalog, hasOtherSchema := false, true, false, false

		for rqfn := range rm.Rels {
			if strings.HasPrefix(rqfn.RelationName, "pg_") {
				anyCatalog = true
			} else {
				onlyCatalog = false
			}
			if rqfn.SchemaName == "information_schema" {
				hasInfSchema = true
			} else {
				hasOtherSchema = true
			}
		}

		if onlyCatalog && anyCatalog {
			return &plan.RandomDispatchPlan{}, nil
		}
		if hasInfSchema && hasOtherSchema {
			return nil, rerrors.ErrInformationSchemaCombinedQuery
		}
		if hasInfSchema {
			return &plan.RandomDispatchPlan{}, nil
		}

		p, err := qr.planQueryV1(ctx, rm, stmt)

		if err != nil {
			return nil, err
		}

		pl = plan.Combine(pl, p)

	case *lyx.Delete, *lyx.Update:
		// UPDATE and/or DELETE, COPY stmts, which
		// would be routed with their WHERE clause
		rs, err := qr.planQueryV1(ctx, rm, stmt)
		if err != nil {
			return nil, err
		}
		pl = plan.Combine(pl, rs)
	case *lyx.ExplainStmt:
		return qr.RouteWithRules(ctx, rm, qs.Query)
	default:
		return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
	}

	return pl, nil
}

func (qr *ProxyQrouter) InitExecutionTargets(ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
	p plan.Plan) (plan.Plan, error) {

	switch v := p.(type) {
	case *plan.DataRowFilter:
		sp, err := qr.InitExecutionTargets(ctx, rm, v.SubPlan)
		if err != nil {
			return nil, err
		}

		/* XXX: Can we do better? */

		return &plan.DataRowFilter{
			SubPlan:     sp,
			FilterIndex: 0,
		}, err
	case *plan.ShardDispatchPlan:
		return v, nil
	case *plan.VirtualPlan:
		return v, nil
	case *plan.RandomDispatchPlan:
		if v.ExecTargets == nil {
			return planner.SelectRandomDispatchPlan(qr.DataShardsRoutes())
		} else {
			/* reference relation case */
			return planner.SelectRandomDispatchPlan(v.ExecTargets)
		}

	case *plan.CopyPlan:
		/* temporary */
		return &plan.ScatterPlan{
			ExecTargets: qr.DataShardsRoutes(),
		}, nil
	case *plan.ScatterPlan:
		if v.IsDDL {
			v.ExecTargets = qr.DataShardsRoutes()
			return v, nil
		}

		if v.Forced {
			if v.ExecTargets == nil {
				v.ExecTargets = qr.DataShardsRoutes()
			}
			return v, nil
		}

		if rm.SPH.EnhancedMultiShardProcessing() {
			var err error
			if v.SubPlan == nil {
				switch rm.Stmt.(type) {
				case *lyx.Select:
				default:
					/* XXX: very dirty hack */
					/* Top level plan */

					plr := planner.PlannerV2{}

					v.SubPlan, err = plr.PlanDistributedQuery(ctx, rm, rm.Stmt, true)
					if err != nil {
						return nil, err
					}
				}
			}
			if v.ExecTargets == nil {
				v.ExecTargets = qr.DataShardsRoutes()
			}
			return v, nil
		}

		/*
		* Here we have a chance for advanced multi-shard query processing.
		* Try to build distributed plan, else scatter-out.
		 */
		switch strings.ToUpper(rm.SPH.DefaultRouteBehaviour()) {
		case "BLOCK":
			return nil, spqrerror.NewByCode(spqrerror.SPQR_NO_DATASHARD)
		case "ALLOW":
			fallthrough
		default:
			if rm.IsRO() {
				/* TODO: config options for this */
				return v, nil
			}
			return nil, spqrerror.NewByCode(spqrerror.SPQR_NO_DATASHARD)
		}
	default:
		return nil, rerrors.ErrComplexQuery
	}
}

func (qr *ProxyQrouter) plannerV1(
	ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
) (plan.Plan, error) {
	/* Top level plan */
	p, err := qr.RouteWithRules(ctx, rm, rm.Stmt)

	if err != nil {
		return nil, err
	}

	tmp, err := rm.RouteByTuples(ctx, rm.SPH.GetTsa())
	if err != nil {
		return nil, err
	}

	p = plan.Combine(p, tmp)

	// set up this variable if not yet
	if p == nil {
		p = &plan.ScatterPlan{
			ExecTargets: qr.DataShardsRoutes(),
		}
	}
	return p, nil
}

func (qr *ProxyQrouter) planSplitUpdate(
	ctx context.Context,
	rm *rmeta.RoutingMetadataContext) (plan.Plan, error) {

	/*
	* Here we want to support distribution column update,
	* i.e. UPDATE <rel> SET <col> = <value> WHERE <col> = <old value>.
	* There can be more that one row which WHERE clause returns.
	* If we allowed do plan split-update, there are still some restrictions.
	* First of all, reject query planing if it has CTE.
	* Also we do not yet support returning for now.
	 */

	stmt := rm.Stmt

	switch q := stmt.(type) {
	case *lyx.Update:

		if q.WithClause != nil {
			return nil, rerrors.ErrComplexQuery
		}
		if q.Returning != nil {
			return nil, rerrors.ErrComplexQuery
		}

		var distribCols []string
		var d *distributions.Distribution
		var r *distributions.DistributedRelation

		var rqdn *rfqn.RelationFQN

		switch tr := q.TableRef.(type) {
		case *lyx.RangeVar:
			rqdn = rfqn.RelationFQNFromRangeRangeVar(tr)

			var err error
			d, err = rm.GetRelationDistribution(ctx, rqdn)
			if err != nil {
				return nil, err
			}

			r = d.GetRelation(rqdn)
			distribCols, err = r.GetDistributionKeyColumns()
			if err != nil {
				return nil, err
			}

			if len(distribCols) != 1 {
				/* TODO: multi-column support here */
				return nil, rerrors.ErrComplexQuery
			}

		default:
			return nil, rerrors.ErrComplexQuery
		}

		var et kr.ShardKey

		/* inner plan */

		inp, err := qr.plannerV1(ctx, rm)
		if err != nil {
			return nil, err
		}

		/* cleanup temporal state */

		/* XXX: introduce Reset() and use */
		rm.ParamRefs = map[rfqn.RelationFQN]map[string][]int{}
		rm.Exprs = map[rfqn.RelationFQN]map[string][]any{}

		for _, c := range q.SetClause {
			switch rt := c.(type) {
			case *lyx.ResTarget:
				if rt.Name == distribCols[0] {

					if err := rm.ProcessConstExprOnRFQN(rqdn, rt.Name, []lyx.Node{rt.Value}); err != nil {
						return nil, err
					}

					spqrlog.Zero.Debug().Msgf("rm params %+v", rm.Exprs)

					queryParamsFormatCodes := prepstatement.GetParams(rm.SPH.BindParamFormatCodes(), rm.SPH.BindParams())

					krs, err := rm.Mgr.ListKeyRanges(ctx, d.Id)
					if err != nil {
						return nil, err
					}

					hf, err := hashfunction.HashFunctionByName(r.DistributionKey[0].HashFunction)
					if err != nil {
						spqrlog.Zero.Debug().Err(err).Msg("failed to resolve hash function")
						return nil, err
					}

					/* len should be one */
					compositeKey := make([]any, len(r.DistributionKey))

					valList, err := rm.ResolveValue(rqdn, rt.Name, queryParamsFormatCodes)

					if err != nil {
						/* Is this ok? */
						return nil, err
					}

					if len(valList) != 1 {
						/* should not happen */
						return nil, rerrors.ErrComplexQuery
					}

					compositeKey[0], err = hashfunction.ApplyHashFunction(valList[0], d.ColTypes[0], hf)

					if err != nil {
						return nil, err
					}

					currroute, err := rm.DeparseKeyWithRangesInternal(ctx, compositeKey, krs)
					if err != nil {
						spqrlog.Zero.Debug().Interface("composite key", compositeKey).Err(err).Msg("encountered the route error")
						return nil, err
					}
					et = currroute

				}
			default:
				return nil, rerrors.ErrComplexQuery
			}
		}

		/* Our top-level plan will be single-shard slice
		* which should be executed where <col> = <value> locates. */
		rPlan := &plan.ShardDispatchPlan{
			ExecTarget: et,
		}

		deleteSliceExecTargets := inp.ExecutionTargets()

		if len(deleteSliceExecTargets) == 1 && deleteSliceExecTargets[0].Name == et.Name {
			return rPlan, nil
		}

		if !config.RouterConfig().Qr.AllowSplitUpdate {
			return nil, spqrerror.Newf(spqrerror.SPQR_NOT_IMPLEMENTED, "updating distribution column is not yet supported")
		}

		/* okay, go throught all execution targets of sub-plan
		* and do query rewrite: we want do DELETE old tuples on source shards
		* as part of split-update. */

		delQuery, err := planner.RewriteUpdateToDelete(rm.Query, rqdn)
		if err != nil {
			return nil, err
		}

		/* Our real rewrite query would be
		*COPY (delstmt RETURNING *) TO STDOUT BINARY.
		 **/

		copyQuery := fmt.Sprintf(`COPY (%s RETURNING *) TO STDOUT BINARY`, delQuery)

		deleteSubplan := &plan.ScatterPlan{
			OverwriteQuery: map[string]string{},
			ExecTargets:    deleteSliceExecTargets,
		}

		for _, et := range deleteSliceExecTargets {
			deleteSubplan.OverwriteQuery[et.Name] = copyQuery
		}

		/* Also define runfunction */

		copyData := make([]pgproto3.CopyData, 0)

		deleteSubplan.RunF = func(serv server.Server) error {
			for _, sh := range serv.Datashards() {

				var errmsg *pgproto3.ErrorResponse

				if !slices.ContainsFunc(deleteSliceExecTargets, func(el kr.ShardKey) bool {
					return sh.Name() == el.Name
				}) {
					continue
				}
			shLoop:
				for {
					msg, err := serv.ReceiveShard(sh.ID())
					if err != nil {
						return err
					}

					switch v := msg.(type) {
					case *pgproto3.ReadyForQuery:
						if v.TxStatus == byte(txstatus.TXERR) {
							return fmt.Errorf("failed to run inner slice, tx status error: %s", errmsg.Message)
						}

						break shLoop
					case *pgproto3.ErrorResponse:
						errmsg = v
					case *pgproto3.CopyData:
						copyData = append(copyData, *v)
					default:
						/* All ok? */
					}
				}
			}

			return nil
		}

		/* This is pretty ugly, fix it someday */
		insertSubplan := &plan.ScatterPlan{
			ExecTargets: []kr.ShardKey{et},
			OverwriteQuery: map[string]string{
				et.Name: fmt.Sprintf(`COPY "%s" FROM STDIN BINARY`, rqdn.String()),
			},
			RunF: func(serv server.Server) error {

				targetShid := uint(0)
				for _, sh := range serv.Datashards() {
					if sh.Name() == et.Name {
						targetShid = sh.ID()
					}
				}

				var errmsg *pgproto3.ErrorResponse

				for _, msg := range copyData {
					if err := serv.SendShard(&msg, et); err != nil {
						return err
					}
				}

				if err := serv.SendShard(&pgproto3.CopyDone{}, et); err != nil {
					return err
				}

				for {
					msg, err := serv.ReceiveShard(targetShid)
					if err != nil {
						return err
					}

					switch v := msg.(type) {
					case *pgproto3.ReadyForQuery:
						if v.TxStatus == byte(txstatus.TXERR) {
							return fmt.Errorf("failed to run inner slice, tx status error: %s", errmsg.Message)
						}

						return nil
					case *pgproto3.ErrorResponse:
						errmsg = v
					default:
						/* All ok? */
					}
				}
			},
		}

		/* Workflow is
		* COPY (DELETE) -> COPY FROM STDIN -> UPDATE on destination shard
		 */

		insertSubplan.SubSlice = deleteSubplan

		rPlan.SP = insertSubplan

		return rPlan, nil

	default:
		return nil, rerrors.ErrComplexQuery
	}

}

func (qr *ProxyQrouter) PlanQueryExtended(
	ctx context.Context,
	rm *rmeta.RoutingMetadataContext) (plan.Plan, error) {

	p, err := rm.ResolveRouteHint(ctx)
	if err != nil {
		return nil, err
	}

	if p != nil {
		return p, nil
	}

	utilityPlan, err := planner.PlanUtility(ctx, rm, rm.Stmt)

	if err != nil {
		return nil, err
	}

	if utilityPlan != nil {
		return utilityPlan, nil
	}

	if rm.IsSplitUpdate {
		return qr.planSplitUpdate(ctx, rm)
	}

	if rm.SPH.PreferredEngine() == planner.EnhancedEngineVersion {

		plr := planner.PlannerV2{}

		p, err = plr.PlanDistributedQuery(ctx, rm, rm.Stmt, true)
		if err != nil {
			return nil, err
		}
	} else {
		/* Top level plan */
		p, err = qr.plannerV1(ctx, rm)
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

func (qr *ProxyQrouter) PlanQueryTopLevel(ctx context.Context, rm *rmeta.RoutingMetadataContext, s lyx.Node) (plan.Plan, error) {
	return qr.planQueryV1(ctx, rm, s)
}

// TODO : unit tests
func (qr *ProxyQrouter) PlanQuery(ctx context.Context, rm *rmeta.RoutingMetadataContext) (plan.Plan, error) {

	if !config.RouterConfig().Qr.AlwaysCheckRules {
		if len(config.RouterConfig().ShardMapping) == 1 {
			firstShard := ""
			for s := range config.RouterConfig().ShardMapping {
				firstShard = s
			}

			return &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: firstShard,
				},
				PStmt: rm.Stmt,
			}, nil
		}
	}

	p, err := qr.PlanQueryExtended(ctx, rm)
	if err != nil {
		return nil, err
	}

	/* Last chance, try to match DRH on some of existing shards */
	for _, sh := range qr.DataShardsRoutes() {
		if sh.Name == rm.SPH.DefaultRouteBehaviour() {
			return &plan.ShardDispatchPlan{
				ExecTarget: sh,
			}, nil
		}
	}

	/* do init plan logic */
	np, err := qr.InitExecutionTargets(ctx, rm, p)
	if err == nil {
		np.SetStmt(rm.Stmt)
	}

	return np, err
}
