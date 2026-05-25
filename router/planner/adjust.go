package planner

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/server"
)

func AdjustPlanStateForFluxAccess(rm *rmeta.RoutingMetadataContext, p plan.Plan) error {

	/* For now, only simple single-shard support */
	dsp, ok := p.(*plan.ShardDispatchPlan)
	if !ok {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "key range is locked")
	}

	if len(rm.RecheckKeyRange) != 1 {
		return spqrerror.New(spqrerror.SPQR_COMPLEX_QUERY, "too many locked key ranges")
	}

	if dsp.SP != nil {
		return spqrerror.New(spqrerror.SPQR_COMPLEX_QUERY, "non-empty subplan")
	}

	dsp.SP = &plan.ShardDispatchPlan{
		OverWriteQuery: fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL TO REPEATABLE READ; SELECT EXISTS(SELECT * FROM spqr_metadata.spqr_local_key_ranges WHERE key_range_id = '%s')", rm.RecheckKeyRange[0].ID),
		ExecTarget:     dsp.ExecTarget,
		RunF: func(serv server.Server) error {
			spqrlog.Zero.Debug().Msg("run bottom-level recheck slice")
			for _, sh := range serv.Datashards() {
				var errmsg *pgproto3.ErrorResponse
				var checkErr error
			shLoop:
				for {
					msg, err := serv.ReceiveShard(sh.ID())
					if err != nil {
						return err
					}

					switch v := msg.(type) {
					case *pgproto3.ReadyForQuery:
						if checkErr != nil {
							return checkErr
						}
						if v.TxStatus == byte(txstatus.TXERR) {
							return fmt.Errorf("failed to run inner slice, tx status error: %s", errmsg.Message)
						}

						break shLoop
					case *pgproto3.DataRow:
						if len(v.Values) == 0 || v.Values[0][0] != 't' {
							checkErr = spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "recheck key range failed")
						}
					case *pgproto3.ErrorResponse:
						errmsg = v
					default:
						/* All ok? */
					}
				}
			}

			return nil
		},
	}

	return nil
}

func AdjustPlanStateForUpsert(rm *rmeta.RoutingMetadataContext, p plan.Plan) error {
	_, ok := p.(*plan.ShardDispatchPlan)
	if ok {
		/* single shard dispatch is safe from hazard upsert */
		return nil
	}

	if rm.HasHazardUpsert {
		p.Hints().AutoLinearize = true
	}

	return nil
}

func AdjustPlanForJoins(ctx context.Context, rm *rmeta.RoutingMetadataContext, p plan.Plan) (plan.Plan, error) {
	sc, ok := p.(*plan.ScatterPlan)
	if !ok {
		return p, nil
	}

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

			if firstKey.CTEName != tmpKey.CTEName || firstKey.ColRefName != tmpKey.ColRefName {
				return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "multiple JOIN rules on AUX CTE").Hint("create issue on github for support")
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

				/* plan adjust for expression routing is not yet supported */
				if dRel.IsExpressionRouting() {
					return p, nil
				}

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

		p, err = RewriteDistributedRelWithValues(rm.Query, firstKey.CTEName, shs, true)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}
