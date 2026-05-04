package planner

import (
	"context"
	"fmt"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rmeta"
)

func AdjustPlanForJoins(ctx context.Context, rm *rmeta.RoutingMetadataContext, p plan.Plan) (plan.Plan, error) {
	sc, ok := p.(*plan.ScatterPlan)
	if !ok {
		return p, nil
	}

	spqrlog.Zero.Debug().Int("used cte", len(rm.UsedAuxCTE)).Msg("adjust scatter query plan for in-shard joins")

	if sc.SubSlice == nil && len(rm.UsedAuxCTE) >= 1 {
		var firstKey rmeta.AuxValuesKey
		var ds *distributions.Distribution
		var hf string
		var err error

		for k, v := range rm.UsedAuxCTE {
			tmpKey := k

			for {
				/* CHECK_FOR_INTERRUPTS :) */
				if v, ok := rm.AuxValuesParent[tmpKey]; ok {
					tmpKey = v
				} else {
					break
				}
			}

			if firstKey.CTEName == "" {
				firstKey = tmpKey
			}

			if firstKey.CTEName != tmpKey.CTEName {
				return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "multiple AUX CTE join rewrite").Hint("create issue on github for support")
			}

			for _, r := range v {
				if rm.RFQNIsCTE(r) {
					continue
				}
				dsTmp, err := rm.GetRelationDistribution(ctx, r)
				if err != nil {
					return nil, err
				}

				if dsTmp.Id == distributions.REPLICATED {
					// skip
					continue
				}

				dRel := dsTmp.GetRelation(r)

				if ds == nil {
					ds = dsTmp
					/* XXX: support multicolumn here? */
					hf = dRel.DistributionKey[0].HashFunction

				} else if ds.Id != dsTmp.Id {
					return nil, fmt.Errorf("query with non-collocated joins")
				}

			}
		}
		/* simple WITH .. AS (VALUES()) SELECT .. JOIN .. on ..  */

		var shs []kr.ShardKey

		cte, ok := rm.CteNames[firstKey.CTEName]
		if !ok {
			return nil, fmt.Errorf("failed to resolve CTE by name %v", firstKey.CTEName)
		}
		values, ok := cte.SubQuery.(*lyx.ValueClause)
		if !ok {
			return nil, rerrors.ErrComplexQuery
		}

		rm.UsedSelectQueryAdjust = true

		/* Okay, do plan adjust now */

		routingListPos := map[string]int{}
		for i, v := range cte.NameList {
			routingListPos[v] = i
		}

		/* XXX: todo - support routing expression? */

		dke := []distributions.DistributionKeyEntry{
			{
				Column:       firstKey.ColRefName,
				HashFunction: hf,
			},
		}

		shs, err = TuplePlansByDistributionEntry(ctx, values.Values, ds, rm, routingListPos, dke)
		if err != nil {
			return nil, err
		}

		p, err = RewriteDistributedRelWithValues(rm.Query, firstKey.CTEName, shs)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}
