package planner

import (
	"context"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/xproto"
)

func GetParams(rm *rmeta.RoutingMetadataContext) []int16 {
	paramsFormatCodes := rm.SPH.BindParamFormatCodes()
	var queryParamsFormatCodes []int16
	paramsLen := len(rm.SPH.BindParams())

	/* https://github.com/postgres/postgres/blob/c65bc2e1d14a2d4daed7c1921ac518f2c5ac3d17/src/backend/tcop/pquery.c#L664-L691 */ /* #no-spell-check-line */
	if len(paramsFormatCodes) > 1 {
		queryParamsFormatCodes = paramsFormatCodes
	} else if len(paramsFormatCodes) == 1 {

		/* single format specified, use for all columns */
		queryParamsFormatCodes = make([]int16, paramsLen)

		for i := range paramsLen {
			queryParamsFormatCodes[i] = paramsFormatCodes[0]
		}
	} else {
		/* use default format for all columns */
		queryParamsFormatCodes = make([]int16, paramsLen)
		for i := range paramsLen {
			queryParamsFormatCodes[i] = xproto.FormatCodeText
		}
	}
	return queryParamsFormatCodes
}

func ProcessInsertFromSelectOffsets(
	ctx context.Context, stmt *lyx.Insert, meta *rmeta.RoutingMetadataContext) ([]int, *rfqn.RelationFQN, *distributions.Distribution, error) {
	insertCols := stmt.Columns

	spqrlog.Zero.Debug().
		Strs("insert columns", insertCols).
		Msg("deparsed insert statement columns")

	// compute matched sharding rule offsets
	offsets := make([]int, 0)
	var curr_rfqn *rfqn.RelationFQN

	switch q := stmt.TableRef.(type) {
	case *lyx.RangeVar:

		spqrlog.Zero.Debug().
			Str("relname", q.RelationName).
			Str("schemaname", q.SchemaName).
			Msg("deparsed insert statement table ref")

		curr_rfqn = rfqn.RelationFQNFromRangeRangeVar(q)

		var ds *distributions.Distribution
		var err error

		if ds, err = meta.GetRelationDistribution(ctx, curr_rfqn); err != nil {
			return nil, nil, nil, err
		}

		/* Omit distributed relations */
		if ds.Id == distributions.REPLICATED {
			/* should not happen */
			return nil, nil, nil, rerrors.ErrComplexQuery
		}

		insertColsPos := map[string]int{}
		for i, c := range insertCols {
			insertColsPos[c] = i
		}

		distributionKey := ds.GetRelation(curr_rfqn).DistributionKey
		// TODO: check mapping by rules with multiple columns
		for _, col := range distributionKey {
			if val, ok := insertColsPos[col.Column]; !ok {
				/* Do not return err here.
				* This particular insert stmt is un-routable, but still, give it a try
				* and continue parsing.
				* Example: INSERT INTO xx SELECT * FROM xx a WHERE a.w_id = 20;
				* we have no insert cols specified, but still able to route on select
				 */
				return nil, curr_rfqn, ds, nil
			} else {
				offsets = append(offsets, val)
			}
		}

		return offsets, curr_rfqn, ds, nil
	default:
		return nil, nil, nil, rerrors.ErrComplexQuery
	}
}
