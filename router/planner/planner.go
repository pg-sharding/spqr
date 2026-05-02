package planner

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/engine"
	"github.com/pg-sharding/spqr/pkg/icp"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/console"
	"github.com/pg-sharding/spqr/router/recovery"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/virtual"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/sethvargo/go-retry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	EnhancedEngineVersion = "v2"
)

type PlannerV2 struct {
	// Nothing?
}

// PlanQueryTopLevel implements QueryPlanner.
func (p *PlannerV2) PlanQueryTopLevel(ctx context.Context, rm *rmeta.RoutingMetadataContext, s lyx.Node) (plan.Plan, error) {
	return p.PlanDistributedQuery(ctx, rm, s, true)
}

// Ready implements QueryPlanner.
func (p *PlannerV2) Ready() bool {
	return true
}

func PlanCreateTable(ctx context.Context, rm *rmeta.RoutingMetadataContext, v *lyx.CreateTable) (*plan.ScatterPlan, error) {
	if distributionID := rm.SPH.AutoDistribution(); distributionID != "" {

		switch q := v.TableRv.(type) {
		case *lyx.RangeVar:

			/* pre-attach relation to its distribution
			 * sic! this is not transactional nor abortable
			 */
			spqrlog.Zero.Debug().Str("relation", q.RelationName).Str("distribution", distributionID).Msg("attaching relation")

			if distributionID == distributions.REPLICATED {
				err := console.CreateReferenceRelation(ctx, rm.Mgr, q)
				if err != nil {
					return nil, err
				}
			} else {
				if v.IfNotExists {
					if d, err := rm.Mgr.GetDistribution(ctx, distributionID); err != nil {
						// ok
						return nil, err
					} else {
						/* Attached */
						if d.GetRelation(rfqn.RelationFQNFromRangeRangeVar(q)) != nil {
							/* ok */
						} else {
							err := console.AlterDistributionAttach(ctx, rm.Mgr, q, distributionID, rm.SPH.DistributionKey())
							if err != nil {
								return nil, err
							}
						}
					}
				} else {
					err := console.AlterDistributionAttach(ctx, rm.Mgr, q, distributionID, rm.SPH.DistributionKey())
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}
	/* TODO: support */
	/*
	 * Disallow to create table which does not contain any sharding column
	 */
	if err := CheckRelationIsRoutable(ctx, rm.Mgr, v); err != nil {
		return nil, err
	}

	/*XXX: fix this */
	p := &plan.ScatterPlan{
		IsDDL: true,
	}

	return p, nil
}

func (p *PlannerV2) PlanReferenceRelationModifyWithSubquery(ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
	qualName *rfqn.RelationFQN, subquery lyx.Node,

	allowRewrite bool) (plan.Plan, error) {

	var shs []kr.ShardKey

	if rmeta.IsRelationCatalog(qualName) {
		shs = nil
	} else {
		r, err := rm.Mgr.GetReferenceRelation(ctx, qualName)
		if err != nil {
			return nil, err
		}
		// column rewrite is not yet supported to planner v2
		if len(r.ColumnSequenceMapping) != 0 && !allowRewrite {
			return nil, rerrors.ErrComplexQuery
		}

		shs = r.ListStorageRoutes()
	}

	if subquery == nil {
		return &plan.ScatterPlan{
			SubPlan: &plan.ModifyTable{
				ExecTargets: shs,
			},
			ExecTargets: shs,
		}, nil
	}
	/* Plan sub-select here. TODO: check that modified relation is a ref relation */
	subPlan, err := p.PlanDistributedQuery(ctx, rm, subquery, allowRewrite)
	if err != nil {
		return nil, err
	}
	switch subPlan.(type) {
	case *plan.VirtualPlan:
		return &plan.ScatterPlan{
			SubPlan: &plan.ModifyTable{
				ExecTargets: shs,
			},
			ExecTargets: shs,
		}, nil
	case *plan.ScatterPlan:
		return &plan.ScatterPlan{
			SubPlan: &plan.ModifyTable{
				ExecTargets: shs,
			},
			ExecTargets: shs,
		}, nil
	default:
		return nil, rerrors.ErrComplexQuery
	}
}

func PlanReferenceRelationInsertValues(ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
	columns []string,
	qualName *rfqn.RelationFQN,
	_ *lyx.ValueClause,
	idCache IdentityRouterCache,
) (plan.Plan, error) {

	rel, err := rm.Mgr.GetReferenceRelation(ctx, qualName)
	if err != nil {
		return nil, err
	}
	q, err := InsertSequenceValue(ctx, rm.Query, columns, rel.ColumnSequenceMapping, idCache)

	if err != nil {
		return nil, err
	}

	mp := map[string]string{}
	for _, sh := range rel.ListStorageRoutes() {
		mp[sh.Name] = q
	}

	return &plan.ScatterPlan{
		OverwriteQuery: mp,
		ExecTargets:    rel.ListStorageRoutes(),
	}, nil
}

func CalculateRoutingListTupleItemValue(
	rm *rmeta.RoutingMetadataContext,
	tp string,
	expr lyx.Node, queryParamsFormatCodes []int16) (any, error) {

	v, err := rmeta.ParseExprValue(tp, expr)
	if err != nil {
		return nil, err
	}

	switch q := v.(type) {
	case rmeta.ParamRef:

		// TODO: switch column type here
		// only works for one value
		ind := q.Indx
		if len(queryParamsFormatCodes) < ind {
			return nil, plan.ErrResolvingValue
		}

		fc := queryParamsFormatCodes[ind]

		singleVal, err := plan.ParseResolveParamValue(fc, ind, tp, rm.SPH.BindParams())
		if err != nil {
			return nil, err
		}

		v = singleVal
	}
	return v, nil
}

func TuplePlansByDistributionEntry(
	ctx context.Context,
	routingList [][]lyx.Node,
	ds *distributions.Distribution,
	rm *rmeta.RoutingMetadataContext,
	routingListPos map[string]int, distribKey []distributions.DistributionKeyEntry) ([]kr.ShardKey, error) {

	krs, err := rm.Mgr.ListKeyRanges(ctx, ds.Id)
	if err != nil {
		return nil, err
	}

	queryParamsFormatCodes := prepstatement.GetParams(rm.SPH.BindParamFormatCodes(), rm.SPH.BindParams())

	tupleShards := make([]kr.ShardKey, len(routingList))
	for i := range routingList {
		tup := make([]any, len(ds.ColTypes))

		for j, tp := range ds.ColTypes {

			/* Do not return err here.
			* This particular insert stmt is un-routable, but still, give it a try
			* and continue parsing.
			* Example: INSERT INTO xx SELECT * FROM xx a WHERE a.w_id = 20;
			* we have no insert cols specified, but still able to route on select
			 */

			hf, err := hashfunction.HashFunctionByName(distribKey[j].HashFunction)
			if err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to resolve hash function")
				return nil, err
			}

			if len(distribKey[j].Column) == 0 {

				if len(distribKey[j].Expr.ColRefs) == 0 {
					return nil, rerrors.ErrComplexQuery
				}

				acc := []byte{}

				for _, cr := range distribKey[j].Expr.ColRefs {

					val, ok := routingListPos[cr.ColName]

					if !ok {
						return nil, nil
					}

					if len(routingList[i]) <= val {
						return nil, nil
					}

					switch routingList[i][val].(type) {
					case *lyx.AExprIConst, *lyx.AExprBConst, *lyx.AExprSConst, *lyx.ParamRef, *lyx.AExprNConst:
					default:
						return nil, nil
					}

					/* this is always non-ident hash function */
					itemVal, err := CalculateRoutingListTupleItemValue(rm,
						cr.ColType,
						routingList[i][val],
						queryParamsFormatCodes)

					if err != nil {
						spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
						return nil, err
					}

					hashedCol, err := hashfunction.ApplyNonIdentHashFunction(itemVal, cr.ColType, hf)

					if err != nil {
						spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
						return nil, err
					}

					acc = append(acc, hashfunction.EncodeUInt64(uint64(hashedCol))...)
				}

				/* because we take hash of bytes */
				tup[j], err = hashfunction.ApplyHashFunction(acc, qdb.ColumnTypeVarcharHashed, hf)

				if err != nil {
					return nil, err
				}

			} else {
				val, ok := routingListPos[distribKey[j].Column]

				if !ok {
					return nil, nil
				}

				if len(routingList[i]) <= val {
					return nil, nil
				}

				switch routingList[i][val].(type) {
				case *lyx.AExprIConst, *lyx.AExprBConst, *lyx.AExprSConst, *lyx.ParamRef, *lyx.AExprNConst:
				default:
					return nil, nil
				}

				itemVal, err := CalculateRoutingListTupleItemValue(rm,
					tp,
					routingList[i][val],
					queryParamsFormatCodes)

				if err != nil {
					return nil, err
				}

				tup[j], err = hashfunction.ApplyHashFunction(itemVal, ds.ColTypes[j], hf)

				if err != nil {
					return nil, err
				}
			}
		}

		tupleShard, err := rm.DeparseKeyWithRangesInternal(ctx, tup, krs)
		if err != nil {
			spqrlog.Zero.Debug().Interface("composite key", tup).Err(err).Msg("encountered the route error")
			return nil, err
		}

		spqrlog.Zero.Debug().
			Interface("tuple shard", tupleShard).
			Msg("calculated route for single insert tuple")

			/* this is modify stmt */
		tupleShards[i] = tupleShard
	}
	return tupleShards, nil
}

func PlanDistributedRelationForKeys(
	ctx context.Context,
	routingList [][]lyx.Node,
	rm *rmeta.RoutingMetadataContext,
	insertColsPos map[string]int, qualName *rfqn.RelationFQN) ([]kr.ShardKey, error) {

	var ds *distributions.Distribution
	var err error

	if ds, err = rm.GetRelationDistribution(ctx, qualName); err != nil {
		return nil, err
	}

	/* Omit distributed relations */
	if ds.Id == distributions.REPLICATED {
		/* should not happen */
		return nil, rerrors.ErrComplexQuery
	}
	relation, ok := ds.TryGetRelation(qualName)
	if !ok {
		return nil, spqrerror.NewByCode(spqrerror.SPQR_NO_DATASHARD)
	}

	return TuplePlansByDistributionEntry(ctx, routingList, ds, rm, insertColsPos, relation.DistributionKey)
}

func parseStringFuncArg(fname string, arg lyx.Node) (string, error) {
	switch vv := arg.(type) {
	case *lyx.AExprSConst:
		return vv.Value, nil
	default:
		return "", spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "wrong argument type for %s", fname)
	}
}

func MetadataVirtualFunctionCall(ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
	plr QueryPlanner,
	fname string,
	args []lyx.Node) (*tupleslot.TupleTableSlot, error) {

	spqrlog.Zero.Debug().Str("func name", fname).Msg("running MetadataVirtualFunctionCall")

	switch fname {
	case virtual.VirtualAwaitTask:

		if len(args) != 1 {
			return nil, fmt.Errorf("%s function only accept single arg", virtual.VirtualAwaitTask)
		}

		mgr, cf, err := coord.DistributedMgr(ctx, rm.Mgr)
		if err != nil {
			return nil, err
		}
		defer cf()

		switch v := args[0].(type) {
		case *lyx.AExprSConst:

			for {
				/* TODO: add CFI here*/

				tg, err := mgr.GetMoveTaskGroup(ctx, v.Value)
				if err != nil {
					/* TODO: better check that err is `not exists` */
					return nil, err
				}
				if tg == nil {
					break
				}

				st, err := mgr.GetTaskGroupStatus(ctx, v.Value)
				if err != nil {
					return nil, err
				}
				/* Still ongoing */
				spqrlog.Zero.Info().Str("id", tg.ID).Str("status", string(st.State)).Msgf("move task still in-progress")

				/* RUNNING or PLANNING here are ok */

				if st.State == tasks.TaskGroupError {
					break
				}

				/* Maybe notify client here */

				time.Sleep(time.Second)
			}

			return &tupleslot.TupleTableSlot{
				Desc: []pgproto3.FieldDescription{},
			}, nil
		default:
			return nil, rerrors.ErrComplexQuery

		}

	case virtual.VirtualConsoleExecute:

		/*  XXX: unite this code with client interactor internals */

		if len(args) != 1 {
			return nil, fmt.Errorf("%s function only accept single arg", virtual.VirtualConsoleExecute)
		}

		switch v := args[0].(type) {
		case *lyx.AExprSConst:

			tstmt, err := spqrparser.Parse(v.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to parse query \"%s\": %w", v.Value, err)
			}

			mgr := rm.Mgr
			var cf func()

			switch tstmt := tstmt.(type) {
			case *spqrparser.Show:
				/* TODO - fix
				if err := gc.CheckGrants(catalog.RoleAdmin, rc.Rule()); err != nil {
					return err
				}
				*/
				switch tstmt.Cmd {
				case spqrparser.RoutersStr, spqrparser.TaskGroupStr, spqrparser.TaskGroupsStr, spqrparser.MoveTaskStr, spqrparser.MoveTasksStr, spqrparser.SequencesStr:
					mgr, cf, err = coord.DistributedMgr(ctx, mgr)
					if err != nil {
						return nil, err
					}
					defer cf()
				}
			default:
				/* TODO - fix
				if err := gc.CheckGrants(catalog.RoleAdmin, rc.Rule()); err != nil {
					return nil, err
				}
				*/
				mgr, cf, err = coord.DistributedMgr(ctx, mgr)
				if err != nil {
					return nil, err
				}
				defer cf()
			}

			return retry.DoValue(ctx, retry.WithMaxRetries(10, retry.NewConstant(time.Second)), func(ctx context.Context) (*tupleslot.TupleTableSlot, error) {
				tts, err := meta.ProcMetadataCommand(ctx, tstmt, mgr, rm.CSM, rm.ClientRule, nil, false)
				if err != nil {
					if st, ok := status.FromError(err); ok && st.Code() == codes.Canceled && st.Message() == "grpc: the client connection is closing" {
						return nil, retry.RetryableError(err)
					}
					return nil, err
				}
				return tts, nil
			})
		default:
			return nil, rerrors.ErrComplexQuery
		}

	case virtual.PGIsolationTestSessionIsBlocked:
		if len(args) != 2 {
			return nil, fmt.Errorf("%s function only accept two arguments", virtual.PGIsolationTestSessionIsBlocked)
		}

		lockedVirtualPID := uint32(0)
		lockedByVirtualPIDs := []uint32{}

		switch v := args[0].(type) {
		case *lyx.AExprIConst:
			lockedVirtualPID = uint32(v.Value)
		case *lyx.ParamRef:

			queryParamsFormatCodes := prepstatement.GetParams(rm.SPH.BindParamFormatCodes(), rm.SPH.BindParams())

			sVal, err := rm.ResolveTypedParamRef(queryParamsFormatCodes, int(v.Number-1), qdb.ColumnTypeUinteger)
			if err != nil {
				return nil, err
			}

			lockedVirtualPID = uint32(sVal.(uint64))
		default:
			return nil, rerrors.ErrComplexQuery
		}

		_ = rm.CSM.ClientPoolForeach(func(client client.ClientInfo) error {
			if client.CancelPID() == lockedVirtualPID {
				lockedByVirtualPIDs = client.CancellableIDs()

				spqrlog.Zero.Debug().Uint32("pid", lockedVirtualPID).Msgf("resolved virtual pid from param: %+v", lockedByVirtualPIDs)
			}
			return nil
		})
		res := byte('f')

		/* Not attached? XXX: fix this, support proper handling of second arg */
		if len(lockedByVirtualPIDs) != 0 {
			res = byte('t')
		}

		if _, ok := icp.BlockedPIDs[lockedVirtualPID]; ok {
			res = byte('t')
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: []pgproto3.FieldDescription{
				{
					Name:                 []byte("locked"),
					DataTypeOID:          catalog.BOOLOID,
					TypeModifier:         -1,
					DataTypeSize:         1,
					TableAttributeNumber: 0,
					TableOID:             0,
					Format:               0,
				},
			},

			Raw: [][][]byte{{{res}}},
		}

		return tts, nil

		/*  De-support? use __spqr__show(shards)*/
	case virtual.VirtualShow:

		/*  XXX: unite this code with client interactor internals */

		if len(args) != 1 {
			return nil, fmt.Errorf("%s function only accept single arg", virtual.VirtualShow)
		}

		var target string

		switch v := args[0].(type) {
		case *lyx.ParamRef:

			queryParamsFormatCodes := prepstatement.GetParams(rm.SPH.BindParamFormatCodes(), rm.SPH.BindParams())

			sVal, err := rm.ResolveTypedParamRef(queryParamsFormatCodes, int(v.Number-1), qdb.ColumnTypeVarchar)
			if err != nil {
				return nil, err
			}

			val, ok := sVal.(string)
			if !ok {
				return nil, fmt.Errorf("expected string param, got %T", sVal)
			}

			target = val
		case *lyx.AExprSConst:
			target = v.Value
		default:
			return nil, rerrors.ErrComplexQuery
		}

		return meta.ProcessShow(ctx, &spqrparser.Show{
			Cmd:     target,
			Where:   &lyx.AExprEmpty{},
			Order:   nil,
			GroupBy: nil,
		}, rm.Mgr, rm.CSM, true)

		/*  De-support? use __spqr__show(shards)*/
	case virtual.VirtualShards:
		tts := &tupleslot.TupleTableSlot{
			Desc: []pgproto3.FieldDescription{
				{
					Name:                 []byte("shard name"),
					DataTypeOID:          catalog.TEXTOID,
					TypeModifier:         -1,
					DataTypeSize:         1,
					TableAttributeNumber: 0,
					TableOID:             0,
					Format:               0,
				},
			},
		}

		shs, err := rm.Mgr.ListShards(ctx)
		if err != nil {
			return nil, err
		}
		for _, sh := range shs {
			tts.Raw = append(tts.Raw, [][]byte{[]byte(sh.ID)})
		}

		return tts, nil
	case virtual.VirtualFuncHosts:
		if rm.CSM == nil {
			return nil, fmt.Errorf("spqr metadata uninitialized")
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: []pgproto3.FieldDescription{
				{
					Name:                 []byte("host"),
					DataTypeOID:          catalog.TEXTOID,
					TypeModifier:         -1,
					DataTypeSize:         1,
					TableAttributeNumber: 0,
					TableOID:             0,
					Format:               0,
				},
				{
					Name:                 []byte("rw"),
					DataTypeOID:          catalog.TEXTOID,
					TypeModifier:         -1,
					DataTypeSize:         1,
					TableAttributeNumber: 0,
					TableOID:             0,
					Format:               0,
				},
			},
		}

		if len(args) != 1 {
			return nil, spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "wrong number of arguments for %s", fname)
		}

		k, err := parseStringFuncArg(fname, args[0])
		if err != nil {
			return nil, err
		}

		if v, ok := rm.CSM.InstanceHealthChecks()[k]; ok {
			tts.Raw = append(tts.Raw,
				[][]byte{[]byte(k),
					fmt.Appendf(nil, "%v", v.CR.RW)})
		} else {
			return nil, spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "wrong first argument for %s", fname)
		}

		return tts, nil
	case virtual.VirtualFuncIsReady:

		tts := &tupleslot.TupleTableSlot{
			Desc: []pgproto3.FieldDescription{pgproto3.FieldDescription{
				Name:                 []byte(virtual.VirtualFuncIsReady),
				DataTypeOID:          catalog.BOOLOID,
				TypeModifier:         -1,
				DataTypeSize:         1,
				TableAttributeNumber: 0,
				TableOID:             0,
				Format:               0,
			},
			},
		}

		if plr.Ready() {
			tts.Raw = [][][]byte{[][]byte{[]byte{'t'}}}
		} else {
			tts.Raw = [][][]byte{[][]byte{[]byte{'f'}}}
		}

		return tts, nil
	case virtual.VirtualRouteKey:

		/* First arg is SPQR distribution name, second arg is
		* routing key itself */
		if len(args) != 2 {
			return nil, spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "wrong number of arguments for %s", fname)
		}

		id, err := parseStringFuncArg(fname, args[0])
		if err != nil {
			return nil, err
		}

		d, err := rm.Mgr.GetDistribution(ctx, id)
		if err != nil {
			return nil, err
		}

		strVal, err := parseStringFuncArg(fname, args[1])
		if err != nil {
			return nil, err
		}

		et, err := rm.ResolveKeyShard(ctx, d, strVal)
		if err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: []pgproto3.FieldDescription{
				{
					Name:                 []byte(fname),
					DataTypeOID:          catalog.TEXTOID,
					TypeModifier:         -1,
					DataTypeSize:         1,
					TableAttributeNumber: 0,
					TableOID:             0,
					Format:               0,
				},
			},
		}

		tts.WriteDataRow(et.Name)

		return tts, nil
	case virtual.VirtualRemoteExecute:
		if len(args) != 2 {
			return nil, fmt.Errorf("%s function accepts two arguments", virtual.VirtualRemoteExecute)
		}
		strVal, ok := args[0].(*lyx.AExprSConst)
		if !ok {
			return nil, rerrors.ErrComplexQuery
		}
		dsn := strVal.Value
		strVal, ok = args[1].(*lyx.AExprSConst)
		if !ok {
			return nil, rerrors.ErrComplexQuery
		}
		queriesStr := strVal.Value
		queries := strings.Split(queriesStr, ";")
		connConfig, err := pgx.ParseConfig(dsn)
		if err != nil {
			return nil, err
		}
		connConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		connConfig.RuntimeParams["standard_conforming_strings"] = "on"
		connConfig.RuntimeParams["idle_session_timeout"] = "10000"
		level, err := tracelog.LogLevelFromString(tracelog.LogLevelDebug.String())
		if err != nil {
			return nil, err
		}
		connConfig.Tracer = &tracelog.TraceLog{
			Logger:   &spqrlog.ZeroTraceLogger{},
			LogLevel: level,
		}
		return func() (*tupleslot.TupleTableSlot, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, err := pgx.ConnectConfig(ctx, connConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to connect to remote host: %w", err)
			}
			tts := &tupleslot.TupleTableSlot{
				Desc: tupleslot.TupleDesc{},
			}
			for i, q := range queries {
				if err := func() error {
					rows, err := conn.Query(ctx, q)
					if err != nil {
						return err
					}
					defer rows.Close()
					for _, desc := range rows.FieldDescriptions() {
						tts.Desc = append(tts.Desc, engine.TextOidFD(desc.Name))
					}
					for rows.Next() {
						if i == len(queries)-1 {
							vals, err := rows.Values()
							if err != nil {
								return err
							}
							strVals := make([]string, len(vals))
							for i, val := range vals {
								strVals[i] = fmt.Sprintf("%v", val)
							}
							tts.WriteDataRow(strVals...)
						}
					}
					return nil
				}(); err != nil {
					return nil, err
				}
			}
			return tts, nil
		}()
	case virtual.VirtualRun2PCRecover:
		if len(args) > 1 {
			return nil, fmt.Errorf("%s function accepts no more than one arg", virtual.VirtualRun2PCRecover)
		}

		wd, err := recovery.NewTwoPCWatchDog(config.RouterConfig().WatchdogBackendRule, topology.TopMgr)
		if err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{Desc: tupleslot.TupleDesc{engine.TextOidFD("run_2pc_recover")}}
		if len(args) == 1 {
			strVal, ok := args[0].(*lyx.AExprSConst)
			if !ok {
				return nil, rerrors.ErrComplexQuery
			}
			gid := strVal.Value

			if err := wd.LockAndRecover2PhaseCommitTX(gid); err != nil {
				return nil, err
			}
			tts.WriteDataRow(gid)
		} else {
			gids, err := wd.RecoverDistributedTx()
			if err != nil {
				return nil, err
			}
			for range gids {
				tts.WriteDataRow("")
			}
		}
		return tts, nil
	case virtual.VirtualClear2PCData:
		if len(args) > 0 {
			return nil, fmt.Errorf("%s function accepts no more than one arg", virtual.VirtualClear2PCData)
		}
		db, err := qdb.GetStateKeeperQDB()
		if err != nil {
			return nil, err
		}
		if err := db.ClearTxStatuses(ctx); err != nil {
			return nil, err
		}
		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("clear_2pc_data"),
		}
		return tts, nil
	case virtual.VirtualCleanOutdated2PCData:
		if len(args) > 0 {
			return nil, fmt.Errorf("%s function accepts no more than one arg", virtual.VirtualCleanOutdated2PCData)
		}
		wd, err := recovery.NewTwoPCWatchDog(config.RouterConfig().WatchdogBackendRule, topology.TopMgr)
		if err != nil {
			return nil, err
		}
		gids, err := wd.CleanUpOldTXs(ctx)
		if err != nil {
			return nil, err
		}
		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("gid"),
		}
		for _, gid := range gids {
			tts.WriteDataRow(gid)
		}
		return tts, nil
	}
	return nil, fmt.Errorf("unknown virtual spqr function: %s", fname)
}

func RetrieveTuples(
	ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
	plr QueryPlanner,
	n lyx.Node) (*tupleslot.TupleTableSlot, error) {
	switch q := n.(type) {
	case *lyx.FuncApplication:
		if virtual.IsVirtualFuncName(q.Name) {
			tts, err := MetadataVirtualFunctionCall(ctx,
				rm, plr, q.Name, q.Args)
			return tts, err
		}
	}
	/* XXX: we should error out here */
	/* other cases unsupported */
	return nil, nil
}

func (p *PlannerV2) PlanDistributedQuery(
	ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
	stmt lyx.Node, allowRewrite bool) (plan.Plan, error) {

	switch v := stmt.(type) {
	/* TDB: comments? */
	case *lyx.ValueClause:
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil
	case *lyx.ExplainStmt:
		return p.PlanDistributedQuery(ctx, rm, v.Query, allowRewrite)
	case *lyx.SubLink:
		return p.PlanDistributedQuery(ctx, rm, v.SubSelect, allowRewrite)
	case *lyx.Select:
		/* Should be single-relation scan or values. Join to be supported */
		if len(v.FromClause) == 0 {

			// try to plan query, if query is virtual-only

			// is query a single function call?

			if len(v.TargetList) == 1 {

				tts, err := RetrieveTuples(ctx, rm, p, v.TargetList[0])
				if err != nil {
					return nil, err
				}

				if tts != nil {
					return &plan.VirtualPlan{
						TTS: tts,
					}, nil
				}
			}

			/* Should we try to recurse here? */
			/* try to run planner on target list  */

			/* We cannot route SQL statements without a FROM clause. However, there are a few cases to consider. */
			if len(v.FromClause) == 0 && (v.LArg == nil || v.RArg == nil) && v.WithClause == nil {
				p, err := PlanTargetList(ctx, rm, p, v)
				if err != nil {
					return nil, err
				}

				return p, nil
			}

			return &plan.ScatterPlan{}, nil
		}

		/* Special case for `select * from __spqr__show('obj')` */

		if len(v.FromClause) == 1 {

			switch q := v.FromClause[0].(type) {
			case *lyx.SubSelect:
				tts, err := RetrieveTuples(
					ctx,
					rm,
					p, q.Arg)
				if err != nil {
					return nil, err
				}
				if tts != nil {
					return &plan.VirtualPlan{
						TTS: tts,
					}, nil
				}
			default:
				break
			}
		}

		if len(v.FromClause) > 1 {
			return nil, rerrors.ErrComplexQuery
		}

		/* we only support reference relation here */

		s := plan.Scan{}
		switch q := v.FromClause[0].(type) {
		case *lyx.RangeVar:
			s.Relation = q

			qualName := rfqn.RelationFQNFromRangeRangeVar(q)

			// CTE, skip
			if rm.RFQNIsCTE(qualName) {
				// is that ok?
				return &plan.ScatterPlan{}, nil
			}

			if ds, err := rm.GetRelationDistribution(ctx, qualName); err != nil {
				return nil, err
			} else if ds.ID() != distributions.REPLICATED {
				return nil, rerrors.ErrComplexQuery
			}
		default:
			return nil, rerrors.ErrComplexQuery
		}

		s.Projection = v.TargetList

		/* Todo: support grouping columns */
		return &plan.ScatterPlan{
			SubPlan: s,
		}, nil
	case *lyx.Insert:
		switch q := v.TableRef.(type) {
		case *lyx.RangeVar:

			qualName := rfqn.RelationFQNFromRangeRangeVar(q)

			if ds, err := rm.GetRelationDistribution(ctx, qualName); err != nil {
				return nil, rerrors.ErrComplexQuery
			} else if ds.Id != distributions.REPLICATED {

				switch q := v.SubSelect.(type) {
				case *lyx.ValueClause:
					if v.WithClause != nil {
						return nil, rerrors.ErrComplexQuery
					}

					insertColsPos, _, err := ProcessInsertFromSelectOffsets(ctx, v, rm)
					if err != nil {
						return nil, err
					}

					shs, err := PlanDistributedRelationForKeys(ctx, q.Values, rm, insertColsPos, qualName)
					if err != nil {
						return nil, err
					}
					/* XXX: give change for engine v2 to rewrite queries */
					for _, sh := range shs {
						if sh.Name != shs[0].Name {
							/* try to rewrite, but only for simple protocol */
							if len(rm.ParamRefs) == 0 {
								return RewriteDistributedRelBatchInsert(rm.Query, shs)
							}
							return nil, rerrors.ErrComplexQuery
						}
					}

					if len(shs) > 0 {
						return &plan.ShardDispatchPlan{
							ExecTarget:         shs[0],
							TargetSessionAttrs: config.TargetSessionAttrsRW,
						}, nil
					}
					return nil, rerrors.ErrComplexQuery

				default:
					/* XXX: support some simple patterns here?  */
					return nil, rerrors.ErrComplexQuery
				}
			}

			return p.PlanReferenceRelationModifyWithSubquery(ctx, rm, qualName, v.SubSelect, allowRewrite)
		default:
			return nil, rerrors.ErrComplexQuery
		}

	case *lyx.Update:
		switch q := v.TableRef.(type) {
		case *lyx.RangeVar:
			qualName := &rfqn.RelationFQN{
				RelationName: q.RelationName,
				SchemaName:   q.SchemaName,
			}

			if ds, err := rm.GetRelationDistribution(ctx, qualName); err != nil {
				return nil, rerrors.ErrComplexQuery
			} else if ds.Id != distributions.REPLICATED {
				return &plan.ScatterPlan{
					SubPlan: &plan.ModifyTable{
						ExecTargets: nil,
					},
					ExecTargets: nil,
				}, nil
			}

			p, err := p.PlanReferenceRelationModifyWithSubquery(ctx, rm, qualName, nil, allowRewrite)
			if v.Returning != nil {
				return &plan.DataRowFilter{
					SubPlan:     p,
					FilterIndex: 0,
				}, nil
			}
			return p, err
		default:
			return nil, rerrors.ErrComplexQuery
		}

	case *lyx.Delete:
		switch q := v.TableRef.(type) {
		case *lyx.RangeVar:

			qualName := &rfqn.RelationFQN{
				RelationName: q.RelationName,
				SchemaName:   q.SchemaName,
			}

			if ds, err := rm.GetRelationDistribution(ctx, qualName); err != nil {
				return nil, rerrors.ErrComplexQuery
			} else if ds.Id != distributions.REPLICATED {
				return &plan.ScatterPlan{
					SubPlan: &plan.ModifyTable{
						ExecTargets: nil,
					},
					ExecTargets: nil,
				}, nil
			}

			p, err := p.PlanReferenceRelationModifyWithSubquery(ctx, rm, qualName, nil, allowRewrite)
			if v.Returning != nil {
				return &plan.DataRowFilter{
					SubPlan:     p,
					FilterIndex: 0,
				}, nil
			}
			return p, err
		default:
			return nil, rerrors.ErrComplexQuery
		}
	default:
		return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
	}
}
