package qrouter

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/engine"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/pg-sharding/spqr/router/xproto"
)

func (qr *ProxyQrouter) addLimitToPlan(
	_ context.Context,
	rm *rmeta.RoutingMetadataContext,
	p plan.Plan,
) (plan.Plan, error) {
	scatterSlice, ok := p.(*plan.ScatterPlan)

	if !ok {
		return p, nil
	}

	spqrlog.Zero.Debug().
		Msgf("plan select limit postprocessing %+v", p)

	switch stmt := rm.Stmt.(type) {
	case *lyx.Select:
		if stmt.Limit == nil {
			return p, nil
		}

		limitVal := int64(0)
		selectLim, ok := stmt.Limit.(*lyx.SelectLimit)
		if !ok {
			return nil, rerrors.ErrComplexQuery
		}

		q, ok := selectLim.LimitCount.(*lyx.AExprIConst)

		if !ok {
			/* no support */
			return p, nil
		}
		limitVal = q.Value

		retSlice := &plan.VirtualPlan{
			TTS: &tupleslot.TupleTableSlot{},
		}

		retSlice.SubPlan = scatterSlice

		scatterSlice.OverwriteQuery = map[string]string{}

		for _, sh := range scatterSlice.ExecTargets {
			scatterSlice.OverwriteQuery[sh.Name] = rm.Query
		}

		scatterSlice.RunF = func(serv server.Server) error {
			spqrlog.Zero.Debug().Msg("run bottom-level plan slice")
			for _, sh := range serv.Datashards() {
				if !slices.ContainsFunc(scatterSlice.ExecTargets, func(el kr.ShardKey) bool {
					return sh.Name() == el.Name
				}) {
					continue
				}

				var errmsg *pgproto3.ErrorResponse
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
					case *pgproto3.RowDescription:
						if len(retSlice.TTS.Desc) == 0 {
							retSlice.TTS.Desc = xproto.CopyFieldDescriptions(v.Fields)
						}
					case *pgproto3.ErrorResponse:
						errmsg = v
					case *pgproto3.DataRow:

						if len(retSlice.TTS.Raw) < int(limitVal) {
							retSlice.TTS.Raw = append(retSlice.TTS.Raw, xproto.CopyByteSlices(v.Values))
						}

					default:
						/* All ok? */
					}
				}
			}

			return nil
		}

		return retSlice, nil
	}
	return p, nil
}

func (qr *ProxyQrouter) addSortToPlan(
	ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
	p plan.Plan,
) (plan.Plan, error) {
	/* No point in cluster-wide sorting */
	if len(p.ExecutionTargets()) == 1 {
		return p, nil
	}

	scatterSlice, ok := p.(*plan.ScatterPlan)

	if !ok {
		return p, nil
	}

	spqrlog.Zero.Debug().
		Msgf("plan select sort postprocessing %+v", p)

	switch stmt := rm.Stmt.(type) {
	case *lyx.Select:
		/* This currently support sorting for one column. */
		for _, n := range stmt.SortClause {
			switch sb := n.(type) {
			case *lyx.SortBy:
				colRef, ok := sb.Node.(*lyx.ColumnRef)

				if !ok {
					return p, nil
				}
				/* We can sort by column reference only if we know type of column.
				* For now, all we know in advance is type of distribution column. */
				relationFQN, err := rm.ResolveRelationByAlias(colRef.TableAlias, colRef.ColName)
				if err != nil || relationFQN == nil {
					/* We can receive `complex query` error from ResolveRelationByAlias.
					* log it and ignore */
					spqrlog.Zero.
						Error().
						Str("alias", colRef.TableAlias).
						Err(err).Msg("failed to resolve relation by alias")
					return p, nil
				}

				d, err := rm.GetRelationDistribution(ctx, relationFQN)
				if err != nil {
					return nil, err
				}
				r, ok := d.TryGetRelation(relationFQN)
				if !ok {
					return p, nil
				}
				tp, ok := r.GetDistributionKeyColumnType(d, colRef.ColName)
				if !ok {
					return p, nil
				}

				/* TODO: refactor this */
				if tp != qdb.ColumnTypeVarchar && tp != qdb.ColumnTypeVarcharHashed && tp != qdb.ColumnTypeVarcharDeprecated {
					return p, nil
				}
				columnOff := -1
				for i, tle := range stmt.TargetList {
					switch cf := tle.(type) {
					case *lyx.ColumnRef:
						if cf.ColName == colRef.ColName {
							columnOff = i
						}
					}
				}

				/* XXX: error out here? */
				if columnOff == -1 {
					return p, nil
				}

				/* Okay, we are ready for result post-processing sort.*/

				retSlice := &plan.VirtualPlan{
					TTS: &tupleslot.TupleTableSlot{},
				}

				retSlice.SubPlan = scatterSlice

				scatterSlice.OverwriteQuery = map[string]string{}

				for _, sh := range scatterSlice.ExecTargets {
					scatterSlice.OverwriteQuery[sh.Name] = rm.Query
				}

				scatterSlice.RunF = func(serv server.Server) error {
					spqrlog.Zero.Debug().Msg("run bottom-level plan slice")
					for _, sh := range serv.Datashards() {
						if !slices.ContainsFunc(scatterSlice.ExecTargets, func(el kr.ShardKey) bool {
							return sh.Name() == el.Name
						}) {
							continue
						}

						var errmsg *pgproto3.ErrorResponse
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
							case *pgproto3.RowDescription:
								if len(retSlice.TTS.Desc) == 0 {
									retSlice.TTS.Desc = xproto.CopyFieldDescriptions(v.Fields)
								}
							case *pgproto3.ErrorResponse:
								errmsg = v
							case *pgproto3.DataRow:
								retSlice.TTS.Raw = append(retSlice.TTS.Raw, xproto.CopyByteSlices(v.Values))
							default:
								/* All ok? */
							}
						}
					}

					retSlice.TTS.Raw, err = engine.ProcessOrderBy(retSlice.TTS.Raw, retSlice.TTS.Desc.GetColumnsMap(), sb)
					if err != nil {
						return err
					}

					return nil
				}

				return retSlice, nil
			default:
				/* ??? */
			}
		}

	}

	return p, nil
}

func (qr *ProxyQrouter) addAggregateToPlan(
	_ context.Context,
	rm *rmeta.RoutingMetadataContext,
	p plan.Plan,
) (plan.Plan, error) {

	/* Immediately return if no actual work to do*/
	if len(p.ExecutionTargets()) <= 1 {
		return p, nil
	}

	/* Detect trivial aggregate for post-processing */

	switch stmt := rm.Stmt.(type) {
	case *lyx.Select:
		/* We only support trivial aggregate without GROUP BY */
		if stmt.GroupBy != nil {
			return nil,
				spqrerror.Newf(spqrerror.SPQR_NOT_IMPLEMENTED,
					"in-router postprocessing for GROUP BY aggregate is not yet supported").
					Hint("turn __spqr__allow_postprocessing to off to retrieve underlaying data rows")
		}

		anyAgg := false
		for _, tle := range stmt.TargetList {
			switch q := tle.(type) {
			case *lyx.FuncApplication:
				/* thats ok */
				if engine.EngineV2AggregateFunction(q.Name) {
					anyAgg = true
				}
			default:
			}
		}

		/* We are good as-is */
		if !anyAgg {
			return p, nil
		}

		aggStates := make([]engine.AggregateState, 0)

		/* Current implementation restriction is all-aggregate in query result relation
		target list. Check that.  */
		for i, tle := range stmt.TargetList {
			switch q := tle.(type) {
			case *lyx.FuncApplication:

				/* thats ok */
				if !engine.EngineV2AggregateFunction(q.Name) {
					return nil,
						spqrerror.Newf(spqrerror.SPQR_NOT_IMPLEMENTED,
							"not supported query aggregate function").
							Detail(fmt.Sprintf("target list entry index %d, function name %s", i, q.Name))
				} else {
					aggState, err := engine.CreateAggregate(q.Name, q.Args)
					if err != nil {
						return nil, err
					}
					aggStates = append(aggStates, aggState)
				}
			default:
				/* everything else is unexpected */
				return nil,
					spqrerror.Newf(spqrerror.SPQR_NOT_IMPLEMENTED,
						"malformed query for aggregate: unexpected non-aggregate function in target list").
						Detail(fmt.Sprintf("target list entry index %d, type %T", i, q))
			}
		}

		scatterSlice, ok := p.(*plan.ScatterPlan)

		if !ok {
			return nil, spqrerror.Newf(spqrerror.SPQR_NOT_IMPLEMENTED,
				"non-scatter subquery is not supported in post-process aggregate").
				Hint("turn __spqr__allow_postprocessing to off to retrieve underlaying data rows")
		}

		retSlice := &plan.VirtualPlan{
			TTS: &tupleslot.TupleTableSlot{},
		}

		retSlice.SubPlan = scatterSlice

		scatterSlice.OverwriteQuery = map[string]string{}

		for _, sh := range scatterSlice.ExecTargets {
			scatterSlice.OverwriteQuery[sh.Name] = rm.Query
		}

		scatterSlice.RunF = func(serv server.Server) error {
			spqrlog.Zero.Debug().Msg("run bottom-level plan slice")
			for _, sh := range serv.Datashards() {
				if !slices.ContainsFunc(scatterSlice.ExecTargets, func(el kr.ShardKey) bool {
					return sh.Name() == el.Name
				}) {
					continue
				}

				var errmsg *pgproto3.ErrorResponse
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
					case *pgproto3.RowDescription:
						if len(retSlice.TTS.Desc) == 0 {
							retSlice.TTS.Desc = xproto.CopyFieldDescriptions(v.Fields)

							for i, agg := range aggStates {
								agg.Init(int(v.Fields[i].DataTypeOID))
							}
						}
					case *pgproto3.ErrorResponse:
						errmsg = v
					case *pgproto3.DataRow:
						for i, agg := range aggStates {
							agg.Aggregate(v.Values[i])
						}
					default:
						/* All ok? */
					}
				}
			}

			datRow := []string{}
			for _, agg := range aggStates {
				datRow = append(datRow, fmt.Sprintf("%+v", agg.Finalize()))
			}

			retSlice.TTS.WriteDataRow(datRow...)

			return nil
		}

		return retSlice, nil

	default:
		return p, nil
	}
}
