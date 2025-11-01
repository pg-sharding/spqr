package rmeta

import (
	"context"
	"fmt"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
)

func (rm *RoutingMetadataContext) routingTuples(ctx context.Context,
	ds *distributions.Distribution, qualName *rfqn.RelationFQN, relation *distributions.DistributedRelation, tsa tsa.TSA) (plan.Plan, error) {

	queryParamsFormatCodes := prepstatement.GetParams(rm.SPH.BindParamFormatCodes(), rm.SPH.BindParams())

	krs, err := rm.Mgr.ListKeyRanges(ctx, ds.Id)
	if err != nil {
		return nil, err
	}

	var rec func(lvl int) error
	var p plan.Plan = nil

	compositeKey := make([]any, len(relation.DistributionKey))

	rec = func(lvl int) error {

		hf, err := hashfunction.HashFunctionByName(relation.DistributionKey[lvl].HashFunction)
		if err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to resolve hash function")
			return err
		}

		if len(relation.DistributionKey[lvl].Column) == 0 {
			// calculate routing expression

			valList, err := rm.ComputeRoutingExpr(
				qualName,
				&relation.DistributionKey[lvl].Expr,
				relation.DistributionKey[lvl].HashFunction,
				queryParamsFormatCodes)

			if err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
				return err
			}

			for _, val := range valList {
				compositeKey[lvl] = val

				if lvl+1 == len(relation.DistributionKey) {
					currroute, err := rm.DeparseKeyWithRangesInternal(ctx, compositeKey, krs)
					if err != nil {
						spqrlog.Zero.Debug().Interface("composite key", compositeKey).Err(err).Msg("encountered the route error")
						return err
					}

					spqrlog.Zero.Debug().
						Interface("current route", currroute).
						Str("table", qualName.RelationName).
						Msg("calculated route for table/cols")

					p = plan.Combine(p, &plan.ShardDispatchPlan{
						ExecTarget:         currroute,
						TargetSessionAttrs: tsa,
					})
				} else {
					if err := rec(lvl + 1); err != nil {
						return err
					}
				}
			}

			return nil
		}
		col := relation.DistributionKey[lvl].Column

		vals, err := rm.ResolveValue(qualName, col, queryParamsFormatCodes)

		if err != nil {
			/* Is this ok? */
			return nil
		}

		/* TODO: correct support for composite keys here */

		for _, val := range vals {
			compositeKey[lvl], err = hashfunction.ApplyHashFunction(val, ds.ColTypes[lvl], hf)

			if err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
				return err
			}

			spqrlog.Zero.Debug().Interface("key", val).Interface("hashed key", compositeKey[lvl]).Msg("applying hash function on key")

			if lvl+1 == len(relation.DistributionKey) {
				currroute, err := rm.DeparseKeyWithRangesInternal(ctx, compositeKey, krs)
				if err != nil {
					spqrlog.Zero.Debug().Interface("composite key", compositeKey).Err(err).Msg("encountered the route error")
					return err
				}

				spqrlog.Zero.Debug().
					Interface("current route", currroute).
					Str("table", qualName.RelationName).
					Msg("calculated route for table/cols")

				p = plan.Combine(p, &plan.ShardDispatchPlan{
					ExecTarget:         currroute,
					TargetSessionAttrs: tsa,
				})

			} else {
				if err := rec(lvl + 1); err != nil {
					return err
				}
			}
		}

		return nil
	}

	if err := rec(0); err != nil {
		return nil, err
	}

	return p, nil
}

func (rm *RoutingMetadataContext) RouteByTuples(ctx context.Context, tsa tsa.TSA) (plan.Plan, error) {

	var queryPlan plan.Plan
	/*
	 * Step 2: traverse all aggregated relation distribution tuples and route on them.
	 */

	for qualName := range rm.Rels {
		// TODO: check by whole RFQN
		ds, err := rm.GetRelationDistribution(ctx, &qualName)
		if err != nil {
			return nil, err
		} else if ds.Id == distributions.REPLICATED {
			var shs []kr.ShardKey
			if IsRelationCatalog(&qualName) {
				shs = nil
			} else {
				r, err := rm.Mgr.GetReferenceRelation(ctx, &qualName)
				if err != nil {
					return nil, err
				}
				shs = r.ListStorageRoutes()
			}

			queryPlan = plan.Combine(queryPlan, &plan.RandomDispatchPlan{
				ExecTargets: shs,
			})
			continue
		}

		relation, exists := ds.TryGetRelation(&qualName)
		if !exists {
			return nil, fmt.Errorf("relation %s not found in distribution %s", qualName.RelationName, ds.Id)
		}

		tmp, err := rm.routingTuples(ctx, ds, &qualName, relation, tsa)
		if err != nil {
			return nil, err
		}

		queryPlan = plan.Combine(queryPlan, tmp)
	}

	return queryPlan, nil
}

func (rm *RoutingMetadataContext) ProcessConstExprOnRFQN(resolvedRelation *rfqn.RelationFQN, colname string, exprs []lyx.Node) error {
	off, tp := rm.GetDistributionKeyOffsetType(resolvedRelation, colname)
	if off == -1 {
		// column not from distr key
		return nil
	}

	for _, expr := range exprs {
		/* simple key-value pair */
		if err := rm.ProcessSingleExpr(resolvedRelation, tp, colname, expr); err != nil {
			return err
		}
	}

	return nil
}

// DeparseExprShardingEntries deparses sharding column entries(column names or aliased column names)
// e.g {fields:{string:{str:"a"}} fields:{string:{str:"i"}} for `WHERE a.i = 1`
// returns alias and column name
func DeparseExprShardingEntries(expr lyx.Node) (string, string, error) {
	switch q := expr.(type) {
	case *lyx.ColumnRef:
		return q.TableAlias, q.ColName, nil
	default:
		return "", "", rerrors.ErrComplexQuery
	}
}

func (rm *RoutingMetadataContext) ProcessConstExpr(alias, colname string, expr lyx.Node) error {
	resolvedRelation, err := rm.ResolveRelationByAlias(alias)

	if err != nil {
		// failed to resolve relation, skip column
		return nil
	}

	return rm.ProcessConstExprOnRFQN(resolvedRelation, colname, []lyx.Node{expr})
}

func (rm *RoutingMetadataContext) ComputeRoutingExpr(
	qualName *rfqn.RelationFQN,
	rExpr *distributions.RoutingExpr,
	hfName string,
	queryParamsFormatCodes []int16,
) ([]any, error) {

	hf, err := hashfunction.HashFunctionByName(hfName)
	if err != nil {
		spqrlog.Zero.Debug().Err(err).Msg("failed to resolve hash function")
		return nil, err
	}

	var ret []any

	var rec func(acc []byte, i int) error

	rec = func(acc []byte, i int) error {

		if i == len(rExpr.ColRefs) {
			b, err := hashfunction.ApplyHashFunction(acc, qdb.ColumnTypeVarcharHashed, hf)
			if err != nil {
				return err
			}
			ret = append(ret, b)
			return nil
		}

		if len(rm.ParamRefs[*qualName][rExpr.ColRefs[i].ColName]) == 0 && len(rm.Exprs[*qualName][rExpr.ColRefs[i].ColName]) == 0 {
			return nil
		}

		vals, err := rm.ResolveValue(qualName, rExpr.ColRefs[i].ColName, queryParamsFormatCodes)
		if err != nil {
			return err
		}

		for _, v := range vals {

			lExpr, err := hashfunction.ApplyNonIdentHashFunction(v, rExpr.ColRefs[i].ColType, hf)
			if err != nil {
				/* Is this ok? */
				return err
			}

			buf := hashfunction.EncodeUInt64(uint64(lExpr))

			localAcc := append(acc, buf...)

			if err := rec(localAcc, i+1); err != nil {
				return err
			}
		}
		return nil
	}

	if err := rec([]byte{}, 0); err != nil {
		return nil, err
	}

	return ret, nil
}
