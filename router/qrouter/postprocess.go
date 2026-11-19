package qrouter

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
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
		if stmt, ok := rm.Stmt.(*lyx.Select); ok {
			if limit, ok := stmt.Limit.(*lyx.SelectLimit); ok && limit.LimitOption == lyx.LIMIT_OPTION_WITH_TIES && len(p.ExecutionTargets()) > 1 {
				return nil, rerrors.NewComplexQueryError()
			}
		}
		return p, nil
	}

	spqrlog.Zero.Debug().
		Msgf("plan select limit postprocessing %+v", p)

	switch stmt := rm.Stmt.(type) {
	case *lyx.Select:
		if stmt.Limit == nil {
			return p, nil
		}

		selectLim, ok := stmt.Limit.(*lyx.SelectLimit)
		if !ok {
			return nil, rerrors.NewComplexQueryError()
		}

		withTies := selectLim.LimitOption == lyx.LIMIT_OPTION_WITH_TIES
		if withTies && len(scatterSlice.ExecTargets) == 1 {
			return p, nil
		}

		limitVal, offset := int64(0), int64(0)
		if withTies {
			var err error
			limitVal, offset, err = withTiesLimits(stmt)
			if err != nil {
				return nil, err
			}
		} else {
			q, ok := selectLim.LimitCount.(*lyx.AExprIConst)
			if !ok {
				return p, nil
			}
			limitVal = q.Value
		}

		query := rm.Query
		if withTies {
			var err error
			query, err = trimWithTiesQuery(query, selectLim)
			if err != nil {
				return nil, err
			}
		}

		retSlice := &plan.VirtualPlan{
			TTS:                     &tupleslot.TupleTableSlot{},
			SendEmptyRowDescription: withTies,
		}

		retSlice.SubPlan = scatterSlice

		scatterSlice.OverwriteQuery = map[string]string{}

		for _, sh := range scatterSlice.ExecTargets {
			scatterSlice.OverwriteQuery[sh.Name] = query
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
						if errmsg != nil {
							return spqrerror.New(errmsg.Code, errmsg.Message)
						}
						if v.TxStatus == byte(txstatus.TXERR) {
							return fmt.Errorf("failed to run inner slice, tx status error")
						}

						break shLoop
					case *pgproto3.RowDescription:
						if len(retSlice.TTS.Desc) == 0 {
							retSlice.TTS.Desc = xproto.CopyFieldDescriptions(v.Fields)
						}
					case *pgproto3.ErrorResponse:
						errmsg = v
					case *pgproto3.DataRow:

						if withTies || int64(len(retSlice.TTS.Raw)) < limitVal {
							retSlice.TTS.Raw = append(retSlice.TTS.Raw, xproto.CopyByteSlices(v.Values))
						}

					default:
						/* All ok? */
					}
				}
			}

			if withTies {
				return applyWithTies(retSlice.TTS, stmt.SortClause, offset, limitVal)
			}
			return nil
		}

		return retSlice, nil
	}
	return p, nil
}

var withTiesClause = regexp.MustCompile(`(?is)^(?:OFFSET\s+[0-9]+(?:\s+ROWS?)?\s+)?FETCH\s+(?:FIRST|NEXT)(?:\s+[0-9]+)?\s+ROWS?\s+WITH\s+TIES(?:\s+OFFSET\s+[0-9]+(?:\s+ROWS?)?)?`)
var withTiesTrailer = regexp.MustCompile(`(?s)^(?:\s|;|/\*.*?\*/|--[^\n]*(?:\n|$))*$`)

func trimWithTiesQuery(query string, limit *lyx.SelectLimit) (string, error) {
	loc := limit.CountLoc
	if limit.LimitOffset != nil && limit.OffsetLoc < loc {
		loc = limit.OffsetLoc
	}
	if loc < 0 || loc > len(query) {
		return "", rerrors.NewComplexQueryError()
	}
	suffix := query[loc:]
	match := withTiesClause.FindStringIndex(suffix)
	if match == nil || !withTiesTrailer.MatchString(suffix[match[1]:]) {
		return "", rerrors.NewComplexQueryError()
	}
	return strings.TrimSpace(query[:loc]), nil
}

func withTiesLimits(stmt *lyx.Select) (int64, int64, error) {
	limit, ok := stmt.Limit.(*lyx.SelectLimit)
	if !ok || len(stmt.SortClause) == 0 {
		return 0, 0, rerrors.NewComplexQueryError()
	}
	count, ok := limit.LimitCount.(*lyx.AExprIConst)
	if !ok || count.Value < 0 {
		return 0, 0, rerrors.NewComplexQueryError()
	}
	offset := int64(0)
	if limit.LimitOffset != nil {
		o, ok := limit.LimitOffset.(*lyx.AExprIConst)
		if !ok || o.Value < 0 {
			return 0, 0, rerrors.NewComplexQueryError()
		}
		offset = o.Value
	}
	return count.Value, offset, nil
}

type withTiesKey struct {
	column     int
	oid        uint32
	descending bool
}

func applyWithTies(tts *tupleslot.TupleTableSlot, order []lyx.Node, offset, count int64) error {
	keys := make([]withTiesKey, 0, len(order))
	for _, node := range order {
		sb, ok := node.(*lyx.SortBy)
		if !ok {
			return rerrors.NewComplexQueryError()
		}
		key := withTiesKey{descending: sb.SortbyDir == lyx.SORTBY_DESC}
		if sb.SortbyDir != lyx.SORTBY_ASC && sb.SortbyDir != lyx.SORTBY_DESC && sb.SortbyDir != lyx.SORTBY_DEFAULT {
			return rerrors.NewComplexQueryError()
		}
		switch v := sb.Node.(type) {
		case *lyx.ColumnRef:
			var err error
			key.column, err = tts.ColNameOffset(v.ColName)
			if err != nil {
				return rerrors.NewComplexQueryError()
			}
		case *lyx.AExprIConst:
			key.column = int(v.Value - 1)
		default:
			return rerrors.NewComplexQueryError()
		}
		if key.column < 0 || key.column >= len(tts.Desc) {
			return rerrors.NewComplexQueryError()
		}
		field := tts.Desc[key.column]
		if field.Format != 0 {
			return rerrors.NewComplexQueryError()
		}
		key.oid = field.DataTypeOID
		switch key.oid {
		case pgtype.TextOID, pgtype.VarcharOID,
			pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
		default:
			return rerrors.NewComplexQueryError()
		}
		keys = append(keys, key)
	}
	for _, row := range tts.Raw {
		for _, key := range keys {
			if key.column >= len(row) || row[key.column] == nil {
				return rerrors.NewComplexQueryError()
			}
			switch key.oid {
			case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
				if _, err := strconv.ParseInt(string(row[key.column]), 10, 64); err != nil {
					return err
				}
			}
		}
	}
	sort.SliceStable(tts.Raw, func(i, j int) bool {
		for _, key := range keys {
			left, right := tts.Raw[i][key.column], tts.Raw[j][key.column]
			comparison := bytes.Compare(left, right)
			switch key.oid {
			case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
				a, _ := strconv.ParseInt(string(left), 10, 64)
				b, _ := strconv.ParseInt(string(right), 10, 64)
				comparison = 0
				if a < b {
					comparison = -1
				} else if a > b {
					comparison = 1
				}
			}
			if comparison != 0 {
				if key.descending {
					return comparison > 0
				}
				return comparison < 0
			}
		}
		return false
	})
	if offset >= int64(len(tts.Raw)) || count == 0 {
		tts.Raw = nil
		return nil
	}
	start := int(offset)
	end := len(tts.Raw)
	if count < int64(end-start) {
		end = start + int(count)
	}
	for end < len(tts.Raw) {
		match := true
		for _, key := range keys {
			if !bytes.Equal(tts.Raw[end-1][key.column], tts.Raw[end][key.column]) {
				match = false
				break
			}
		}
		if !match {
			break
		}
		end++
	}
	tts.Raw = tts.Raw[start:end]
	return nil
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
	if stmt, ok := rm.Stmt.(*lyx.Select); ok {
		if limit, ok := stmt.Limit.(*lyx.SelectLimit); ok && limit.LimitOption == lyx.LIMIT_OPTION_WITH_TIES {
			return p, nil
		}
	}

	switch v := p.(type) {
	case *plan.VirtualPlan:
		switch stmt := rm.Stmt.(type) {
		case *lyx.Select:
			/* sort keys in reverse order for correct multi-key ORDER BY */
			for i := len(stmt.SortClause) - 1; i >= 0; i-- {
				sb, ok := stmt.SortClause[i].(*lyx.SortBy)
				if !ok {
					continue
				}
				var err error
				v.TTS.Raw, err = engine.ProcessOrderBy(v.TTS.Raw, v.TTS.Desc.GetColumnsMap(), sb)
				if err != nil {
					return nil, err
				}
			}
		}
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
