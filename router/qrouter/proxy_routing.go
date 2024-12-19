package qrouter

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pg-sharding/spqr/router/routehint"
	"github.com/pg-sharding/spqr/router/routingstate"
	"github.com/pg-sharding/spqr/router/xproto"

	"github.com/pg-sharding/lyx/lyx"
)

type RelationFQN struct {
	RelationName string
	SchemaName   string
}

func RelationFQNFromRangeRangeVar(rv *lyx.RangeVar) RelationFQN {
	return RelationFQN{
		RelationName: rv.RelationName,
		SchemaName:   rv.SchemaName,
	}
}

type RoutingMetadataContext struct {
	// this maps table names to its query-defined restrictions
	// All columns in query should be considered in context of its table,
	// to distinguish composite join/select queries routing schemas
	//
	// For example,
	// SELECT * FROM a join b WHERE a.c1 = <val> and b.c2 = <val>
	// and
	// SELECT * FROM a join b WHERE a.c1 = <val> and a.c2 = <val>
	// can be routed with different rules
	rels      map[RelationFQN]struct{}
	exprs     map[RelationFQN]map[string][]interface{}
	paramRefs map[RelationFQN]map[string][]int

	// cached CTE names
	cteNames map[string]struct{}

	// needed to parse
	// SELECT * FROM t1 a where a.i = 1
	// rarg:{range_var:{relname:"t2" inh:true relpersistence:"p" alias:{aliasname:"b"}
	tableAliases map[string]RelationFQN

	distributions map[RelationFQN]*distributions.Distribution
}

func NewRoutingMetadataContext() *RoutingMetadataContext {
	return &RoutingMetadataContext{
		rels:          map[RelationFQN]struct{}{},
		cteNames:      map[string]struct{}{},
		tableAliases:  map[string]RelationFQN{},
		exprs:         map[RelationFQN]map[string][]interface{}{},
		paramRefs:     map[RelationFQN]map[string][]int{},
		distributions: map[RelationFQN]*distributions.Distribution{},
	}
}

var CatalogDistribution = distributions.Distribution{
	Relations: nil,
	Id:        distributions.REPLICATED,
	ColTypes:  nil,
}

func (meta *RoutingMetadataContext) GetRelationDistribution(ctx context.Context, mgr meta.EntityMgr, resolvedRelation RelationFQN) (*distributions.Distribution, error) {
	if res, ok := meta.distributions[resolvedRelation]; ok {
		return res, nil
	}

	if len(resolvedRelation.RelationName) >= 3 && resolvedRelation.RelationName[0:3] == "pg_" {
		return &CatalogDistribution, nil
	}

	if resolvedRelation.SchemaName == "information_schema" {
		return &CatalogDistribution, nil
	}

	ds, err := mgr.GetRelationDistribution(ctx, resolvedRelation.RelationName)

	if err != nil {
		return nil, err
	}

	meta.distributions[resolvedRelation] = ds
	return ds, nil
}

func (meta *RoutingMetadataContext) RFQNIsCTE(resolvedRelation RelationFQN) bool {
	_, ok := meta.cteNames[resolvedRelation.RelationName]
	return len(resolvedRelation.SchemaName) == 0 && ok
}

// TODO : unit tests
func (meta *RoutingMetadataContext) RecordConstExpr(resolvedRelation RelationFQN, colname string, expr interface{}) error {
	if meta.RFQNIsCTE(resolvedRelation) {
		// CTE, skip
		return nil
	}
	meta.rels[resolvedRelation] = struct{}{}
	if _, ok := meta.exprs[resolvedRelation]; !ok {
		meta.exprs[resolvedRelation] = map[string][]interface{}{}
	}
	if _, ok := meta.exprs[resolvedRelation][colname]; !ok {
		meta.exprs[resolvedRelation][colname] = make([]interface{}, 0)
	}
	meta.exprs[resolvedRelation][colname] = append(meta.exprs[resolvedRelation][colname], expr)
	return nil
}

func (meta *RoutingMetadataContext) RecordParamRefExpr(resolvedRelation RelationFQN, colname string, ind int) error {
	if meta.RFQNIsCTE(resolvedRelation) {
		// CTE, skip
		return nil
	}

	if meta.distributions[resolvedRelation].Id == distributions.REPLICATED {
		// referencr relation, skip
		return nil
	}

	meta.rels[resolvedRelation] = struct{}{}
	if _, ok := meta.paramRefs[resolvedRelation]; !ok {
		meta.paramRefs[resolvedRelation] = map[string][]int{}
	}
	if _, ok := meta.paramRefs[resolvedRelation][colname]; !ok {
		meta.paramRefs[resolvedRelation][colname] = make([]int, 0)
	}
	meta.paramRefs[resolvedRelation][colname] = append(meta.paramRefs[resolvedRelation][colname], ind)
	return nil
}

// TODO : unit tests
func (meta *RoutingMetadataContext) ResolveRelationByAlias(alias string) (RelationFQN, error) {
	if _, ok := meta.rels[RelationFQN{RelationName: alias}]; ok {
		return RelationFQN{RelationName: alias}, nil
	}
	if resolvedRelation, ok := meta.tableAliases[alias]; ok {
		// TBD: postpone routing from here to root of parsing tree
		return resolvedRelation, nil
	} else {
		// TBD: postpone routing from here to root of parsing tree
		if len(meta.rels) != 1 {
			// ambiguity in column aliasing
			return RelationFQN{}, ErrComplexQuery
		}
		for tbl := range meta.rels {
			resolvedRelation = tbl
		}
		return resolvedRelation, nil
	}
}

var ErrComplexQuery = fmt.Errorf("too complex query to route")
var ErrInformationSchemaCombinedQuery = fmt.Errorf("combined information schema and regular relation is not supported")

// DeparseExprShardingEntries deparses sharding column entries(column names or aliased column names)
// e.g {fields:{string:{str:"a"}} fields:{string:{str:"i"}} for `WHERE a.i = 1`
// returns alias and column name
func (qr *ProxyQrouter) DeparseExprShardingEntries(expr lyx.Node, meta *RoutingMetadataContext) (string, string, error) {
	switch q := expr.(type) {
	case *lyx.ColumnRef:
		return q.TableAlias, q.ColName, nil
	default:
		return "", "", ErrComplexQuery
	}
}

// TODO : unit tests
func (qr *ProxyQrouter) DeparseKeyWithRangesInternal(_ context.Context, key []interface{}, krs []*kr.KeyRange) (*routingstate.DataShardRoute, error) {
	spqrlog.Zero.Debug().
		Interface("key", key[0]).
		Int("key-ranges-count", len(krs)).
		Msg("checking key with key ranges")

	var matchedKrkey *kr.KeyRange = nil

	for _, krkey := range krs {
		if kr.CmpRangesLessEqual(krkey.LowerBound, key, krkey.ColumnTypes) &&
			(matchedKrkey == nil || kr.CmpRangesLessEqual(matchedKrkey.LowerBound, krkey.LowerBound, krkey.ColumnTypes)) {
			matchedKrkey = krkey
		}
	}

	if matchedKrkey != nil {
		if err := qr.mgr.ShareKeyRange(matchedKrkey.ID); err != nil {
			return nil, err
		}
		return &routingstate.DataShardRoute{
			Shkey:     kr.ShardKey{Name: matchedKrkey.ShardID},
			Matchedkr: matchedKrkey,
		}, nil
	}
	spqrlog.Zero.Debug().Msg("failed to match key with ranges")

	return nil, fmt.Errorf("failed to match key with ranges")
}

func (qr *ProxyQrouter) GetDistributionKeyOffsetType(meta *RoutingMetadataContext, resolvedRelation RelationFQN, colname string) (int, string) {
	/* do not process non-distributed relations or columns not from relation distribution key */

	ds, err := meta.GetRelationDistribution(context.TODO(), qr.Mgr(), resolvedRelation)
	if err != nil {
		return -1, ""
	} else if ds.Id == distributions.REPLICATED {
		return -1, ""
	}
	// TODO: optimize
	relation, exists := ds.Relations[resolvedRelation.RelationName]
	if !exists {
		return -1, ""
	}
	for ind, c := range relation.DistributionKey {
		if c.Column == colname {
			return ind, ds.ColTypes[ind]
		}
	}
	return -1, ""
}

func (qr *ProxyQrouter) processConstExpr(alias, colname string, expr lyx.Node, meta *RoutingMetadataContext) error {
	resolvedRelation, err := meta.ResolveRelationByAlias(alias)
	if err != nil {
		// failed to resolve relation, skip column
		return nil
	}

	return qr.processConstExprOnRFQN(resolvedRelation, colname, expr, meta)
}

func (qr *ProxyQrouter) processConstExprOnRFQN(resolvedRelation RelationFQN, colname string, expr lyx.Node, meta *RoutingMetadataContext) error {
	off, tp := qr.GetDistributionKeyOffsetType(meta, resolvedRelation, colname)
	if off == -1 {
		// column not from distr key
		return nil
	}

	/* simple key-value pair */
	switch rght := expr.(type) {
	case *lyx.ParamRef:
		return meta.RecordParamRefExpr(resolvedRelation, colname, rght.Number-1)
	case *lyx.AExprSConst:
		switch tp {
		case qdb.ColumnTypeVarcharDeprecated:
			fallthrough
		case qdb.ColumnTypeVarcharHashed:
			fallthrough
		case qdb.ColumnTypeVarchar:
			return meta.RecordConstExpr(resolvedRelation, colname, rght.Value)
		case qdb.ColumnTypeInteger:
			num, err := strconv.ParseInt(rght.Value, 10, 64)
			if err != nil {
				return err
			}
			return meta.RecordConstExpr(resolvedRelation, colname, num)
		case qdb.ColumnTypeUinteger:
			num, err := strconv.ParseUint(rght.Value, 10, 64)
			if err != nil {
				return err
			}
			return meta.RecordConstExpr(resolvedRelation, colname, num)
		default:
			return fmt.Errorf("incorrect key-offset type for AExprSConst expression: %s", tp)
		}
	case *lyx.AExprIConst:
		switch tp {
		case qdb.ColumnTypeVarcharDeprecated:
			fallthrough
		case qdb.ColumnTypeVarcharHashed:
			fallthrough
		case qdb.ColumnTypeVarchar:
			return fmt.Errorf("varchar type is not supported for AExprIConst expression")
		case qdb.ColumnTypeInteger:
			return meta.RecordConstExpr(resolvedRelation, colname, int64(rght.Value))
		case qdb.ColumnTypeUinteger:
			return meta.RecordConstExpr(resolvedRelation, colname, uint64(rght.Value))
		default:
			return fmt.Errorf("incorrect key-offset type for AExprIConst expression: %s", tp)
		}
	default:
		return fmt.Errorf("expression is not const")
	}
}

// routeByClause de-parses sharding column-value pair from Where clause of the query
// TODO : unit tests
func (qr *ProxyQrouter) routeByClause(ctx context.Context, expr lyx.Node, meta *RoutingMetadataContext) error {

	queue := make([]lyx.Node, 0)
	queue = append(queue, expr)

	for len(queue) != 0 {
		var curr lyx.Node
		curr, queue = queue[len(queue)-1], queue[:len(queue)-1]

		switch texpr := curr.(type) {
		case *lyx.AExprOp:

			switch lft := texpr.Left.(type) {
			case *lyx.ColumnRef:

				alias, colname := lft.TableAlias, lft.ColName

				/* simple key-value pair */
				switch rght := texpr.Right.(type) {
				case *lyx.ParamRef, *lyx.AExprSConst, *lyx.AExprIConst:
					// else  error out?

					// TBD: postpone routing from here to root of parsing tree
					// maybe expimely inefficient. Will be fixed in SPQR-2.0
					if err := qr.processConstExpr(alias, colname, rght, meta); err != nil {
						return err
					}

				case *lyx.AExprList:
					if len(rght.List) != 0 {
						expr := rght.List[0]
						if err := qr.processConstExpr(alias, colname, expr, meta); err != nil {
							return err
						}
					}
				case *lyx.FuncApplication:
					// there are several types of queries like DELETE FROM rel WHERE colref = func_applicion
					// and func_applicion is actually routable statement.
					// ANY(ARRAY(subselect)) if one type.

					if strings.ToLower(rght.Name) == "any" {
						if len(rght.Args) > 0 {
							// maybe we should consider not only first arg.
							// however, consider only it

							switch argexpr := rght.Args[0].(type) {
							case *lyx.SubLink:

								// ignore all errors.
								_ = qr.DeparseSelectStmt(ctx, argexpr.SubSelect, meta)
							}
						}
					}

				default:
					queue = append(queue, texpr.Left, texpr.Right)
				}
			case *lyx.Select:
				if err := qr.DeparseSelectStmt(ctx, lft, meta); err != nil {
					return err
				}
			default:
				/* Consider there cases */
				// if !(texpr.AExpr.Kind == pgquery.A_Expr_Kind_AEXPR_OP || texpr.Kind == pgquery.A_Expr_Kind_AEXPR_BETWEEN) {
				// 	return ComplexQuery
				// }
				if texpr.Left != nil {
					queue = append(queue, texpr.Left)
				}
				if texpr.Right != nil {
					queue = append(queue, texpr.Right)
				}
			}
		case *lyx.ColumnRef:
			/* colref = colref case, skip */
		case *lyx.AExprIConst, *lyx.AExprSConst, *lyx.AExprBConst, *lyx.AExprNConst:
			/* should not happend */
		case *lyx.AExprEmpty:
			/*skip*/
		default:
			return ErrComplexQuery
		}
	}
	return nil
}

// TODO : unit tests
func (qr *ProxyQrouter) DeparseSelectStmt(ctx context.Context, selectStmt lyx.Node, meta *RoutingMetadataContext) error {
	switch s := selectStmt.(type) {
	case *lyx.Select:
		if clause := s.FromClause; clause != nil {
			// route `insert into rel select from` stmt
			if err := qr.deparseFromClauseList(ctx, clause, meta); err != nil {
				return err
			}
		}

		if clause := s.Where; clause != nil {
			spqrlog.Zero.Debug().
				Interface("clause", clause).
				Msg("deparsing select where clause")

			if err := qr.routeByClause(ctx, clause, meta); err == nil {
				return nil
			}
		}

		if s.LArg != nil {
			if err := qr.DeparseSelectStmt(ctx, s.LArg, meta); err != nil {
				return err
			}
		}
		if s.RArg != nil {
			if err := qr.DeparseSelectStmt(ctx, s.RArg, meta); err != nil {
				return err
			}
		}

	/* SELECT * FROM VALUES() ... */
	case *lyx.ValueClause:
		/* random route */
		return nil
	}

	return ErrComplexQuery
}

// TODO : unit tests
// deparses from clause
func (qr *ProxyQrouter) deparseFromNode(ctx context.Context, node lyx.FromClauseNode, meta *RoutingMetadataContext) error {
	spqrlog.Zero.Debug().
		Type("node-type", node).
		Msg("deparsing from node")
	switch q := node.(type) {
	case *lyx.RangeVar:
		rqdn := RelationFQNFromRangeRangeVar(q)

		// CTE, skip
		if meta.RFQNIsCTE(rqdn) {
			return nil
		}

		if _, err := meta.GetRelationDistribution(ctx, qr.Mgr(), rqdn); err != nil {
			return err
		}

		if _, ok := meta.rels[rqdn]; !ok {
			meta.rels[rqdn] = struct{}{}
		}
		if q.Alias != "" {
			/* remember table alias */
			meta.tableAliases[q.Alias] = RelationFQNFromRangeRangeVar(q)
		}
	case *lyx.JoinExpr:
		if err := qr.deparseFromNode(ctx, q.Rarg, meta); err != nil {
			return err
		}
		if err := qr.deparseFromNode(ctx, q.Larg, meta); err != nil {
			return err
		}
	default:
		// other cases to consider
		// lateral join, natual, etc

	}

	return nil
}

// TODO : unit tests
func (qr *ProxyQrouter) deparseFromClauseList(
	ctx context.Context,
	clause []lyx.FromClauseNode, meta *RoutingMetadataContext) error {
	for _, node := range clause {
		err := qr.deparseFromNode(ctx, node, meta)
		if err != nil {
			return err
		}
	}

	return nil
}

type StatementRelation interface {
	iRelation()
}

type AnyRelation struct{}
type SpecificRelation struct {
	Name string
}
type RelationList struct {
	Relations []string
}

func (r AnyRelation) iRelation()      {}
func (r SpecificRelation) iRelation() {}
func (r RelationList) iRelation()     {}

var _ StatementRelation = AnyRelation{}
var _ StatementRelation = SpecificRelation{}

func (qr *ProxyQrouter) deparseInsertFromSelectOffsets(ctx context.Context, stmt *lyx.Insert, meta *RoutingMetadataContext) ([]int, RelationFQN, bool, error) {
	insertCols := stmt.Columns

	spqrlog.Zero.Debug().
		Strs("insert columns", insertCols).
		Msg("deparsed insert statement columns")

	// compute matched sharding rule offsets
	offsets := make([]int, 0)
	var rfqn RelationFQN

	switch q := stmt.TableRef.(type) {
	case *lyx.RangeVar:
		rfqn = RelationFQNFromRangeRangeVar(q)

		var ds *distributions.Distribution
		var err error

		if ds, err = meta.GetRelationDistribution(ctx, qr.Mgr(), rfqn); err != nil {
			return nil, RelationFQN{}, false, err
		}

		/* Omit distributed relations */
		if ds.Id == distributions.REPLICATED {
			return nil, RelationFQN{}, false, err
		}

		insertColsPos := map[string]int{}
		for i, c := range insertCols {
			insertColsPos[c] = i
		}

		distributionKey := ds.Relations[rfqn.RelationName].DistributionKey
		// TODO: check mapping by rules with multiple columns
		for _, col := range distributionKey {
			if val, ok := insertColsPos[col.Column]; !ok {
				/* Do not return err here.
				* This particulat insert stmt is un-routable, but still, give it a try
				* and continue parsing.
				* Example: INSERT INTO xx SELECT * FROM xx a WHERE a.w_id = 20;
				* we have no insert cols specified, but still able to route on select
				 */
				return nil, RelationFQN{}, false, nil
			} else {
				offsets = append(offsets, val)
			}
		}

		return offsets, rfqn, true, nil
	default:
		return nil, RelationFQN{}, false, ErrComplexQuery
	}
}

// TODO : unit tests
func (qr *ProxyQrouter) deparseShardingMapping(
	ctx context.Context,
	qstmt lyx.Node,
	meta *RoutingMetadataContext) error {
	switch stmt := qstmt.(type) {
	case *lyx.Select:
		if stmt.WithClause != nil {
			for _, cte := range stmt.WithClause {
				meta.cteNames[cte.Name] = struct{}{}
				if err := qr.deparseShardingMapping(ctx, cte.SubQuery, meta); err != nil {
					return err
				}
			}
		}

		if stmt.FromClause != nil {
			// collect table alias names, if any
			// for single-table queries, process as usual
			if err := qr.deparseFromClauseList(ctx, stmt.FromClause, meta); err != nil {
				return err
			}
		}
		if stmt.Where == nil {
			return nil
		}

		return qr.routeByClause(ctx, stmt.Where, meta)

	case *lyx.Insert:
		if selectStmt := stmt.SubSelect; selectStmt != nil {

			insertCols := stmt.Columns

			switch subS := selectStmt.(type) {
			case *lyx.Select:
				spqrlog.Zero.Debug().Msg("routing insert stmt on select clause")
				_ = qr.DeparseSelectStmt(ctx, subS, meta)
				/* try target list */
				spqrlog.Zero.Debug().Msg("routing insert stmt on target list")
				/* this target list for some insert (...) sharding column */

				offsets, rfqn, success, err := qr.deparseInsertFromSelectOffsets(ctx, stmt, meta)
				if err != nil {
					return err
				}
				if !success {
					break
				}

				targetList := subS.TargetList
				/* record all values from tl */

				tlUsable := true
				for i := range offsets {
					if offsets[i] >= len(targetList) {
						tlUsable = false
						break
					} else {
						switch targetList[offsets[i]].(type) {
						case *lyx.AExprIConst, *lyx.AExprBConst, *lyx.AExprSConst, *lyx.ParamRef, *lyx.AExprNConst:
						default:
							tlUsable = false
						}
					}
				}

				if tlUsable {
					for i := range offsets {
						_ = qr.processConstExprOnRFQN(rfqn, insertCols[offsets[i]], targetList[offsets[i]], meta)
					}
				}

			case *lyx.ValueClause:
				valList := subS.Values
				/* record all values from values scan */
				offsets, rfqn, success, err := qr.deparseInsertFromSelectOffsets(ctx, stmt, meta)
				if err != nil {
					return err
				}
				if !success {
					break
				}

				vlUsable := true
				for i := range offsets {
					if offsets[i] >= len(valList) {
						vlUsable = false
						break
					}
				}

				if vlUsable {
					for i := range offsets {
						_ = qr.processConstExprOnRFQN(rfqn, insertCols[offsets[i]], valList[offsets[i]], meta)
					}
				}
			}
		}

		return nil
	case *lyx.Update:
		clause := stmt.Where
		if clause == nil {
			return nil
		}

		_ = qr.deparseFromNode(ctx, stmt.TableRef, meta)
		return qr.routeByClause(ctx, clause, meta)
	case *lyx.Delete:
		clause := stmt.Where
		if clause == nil {
			return nil
		}

		_ = qr.deparseFromNode(ctx, stmt.TableRef, meta)

		return qr.routeByClause(ctx, clause, meta)
	}

	return nil
}

// CheckTableIsRoutable Given table create statement, check if it is routable with some sharding rule
// TODO : unit tests
func (qr *ProxyQrouter) CheckTableIsRoutable(ctx context.Context, node *lyx.CreateTable) error {
	var err error
	var ds *distributions.Distribution
	var relname string
	switch q := node.TableRv.(type) {
	case *lyx.RangeVar:
		relname = q.RelationName
		ds, err = qr.mgr.GetRelationDistribution(ctx, relname)
		if err != nil {
			return err
		}
		if ds.Id == distributions.REPLICATED {
			return nil
		}
	default:
		return fmt.Errorf("wrong type of table range var")
	}

	entries := make(map[string]struct{})
	/* Collect sharding rule entries list from create statement */
	for _, elt := range node.TableElts {
		// hashing function name unneeded for sharding rules matching purpose
		switch q := elt.(type) {
		case *lyx.TableElt:
			entries[q.ColName] = struct{}{}
		}
	}

	rel, ok := ds.Relations[relname]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "relation \"%s\" not present in distribution \"%s\" it's attached to", relname, ds.Id)
	}
	check := true
	for _, entry := range rel.DistributionKey {
		if _, ok = entries[entry.Column]; !ok {
			check = false
			break
		}
	}
	if check {
		return nil
	}

	return fmt.Errorf("create table stmt ignored: no sharding rule columns found")
}

func (qr *ProxyQrouter) resolveValue(meta *RoutingMetadataContext, rfqn RelationFQN, col string, bindParams [][]byte, paramResCodes []int16) ([]interface{}, bool) {

	if vals, ok := meta.exprs[rfqn][col]; ok {
		return vals, true
	}

	inds, ok := meta.paramRefs[rfqn][col]
	if !ok {
		return nil, false
	}

	off, tp := qr.GetDistributionKeyOffsetType(meta, rfqn, col)
	if off == -1 {
		// column not from distr key
		return nil, false
	}

	// TODO: switch column type here
	// only works for one value
	ind := inds[0]
	fc := paramResCodes[ind]

	switch fc {
	case xproto.FormatCodeBinary:
		switch tp {
		case qdb.ColumnTypeVarcharDeprecated:
			fallthrough
		case qdb.ColumnTypeVarcharHashed:
			fallthrough
		case qdb.ColumnTypeVarchar:
			return []interface{}{string(bindParams[ind])}, true
		case qdb.ColumnTypeInteger:

			var num int64
			var err error

			buf := bytes.NewBuffer(bindParams[ind])

			if len(bindParams[ind]) == 4 {
				var tmpnum int32
				err = binary.Read(buf, binary.BigEndian, &tmpnum)
				num = int64(tmpnum)
			} else {
				err = binary.Read(buf, binary.BigEndian, &num)
			}
			if err != nil {
				return nil, false
			}

			return []interface{}{num}, true
		case qdb.ColumnTypeUinteger:

			var num uint64
			var err error

			buf := bytes.NewBuffer(bindParams[ind])

			if len(bindParams[ind]) == 4 {
				var tmpnum uint32
				err = binary.Read(buf, binary.BigEndian, &tmpnum)
				num = uint64(tmpnum)
			} else {
				err = binary.Read(buf, binary.BigEndian, &num)
			}
			if err != nil {
				return nil, false
			}

			return []interface{}{num}, true
		}
	case xproto.FormatCodeText:
		switch tp {
		case qdb.ColumnTypeVarcharDeprecated:
			fallthrough
		case qdb.ColumnTypeVarcharHashed:
			fallthrough
		case qdb.ColumnTypeVarchar:
			return []interface{}{string(bindParams[ind])}, true
		case qdb.ColumnTypeInteger:
			num, err := strconv.ParseInt(string(bindParams[ind]), 10, 64)
			if err != nil {
				return nil, false
			}
			return []interface{}{num}, true
		case qdb.ColumnTypeUinteger:
			num, err := strconv.ParseUint(string(bindParams[ind]), 10, 64)
			if err != nil {
				return nil, false
			}
			return []interface{}{num}, true
		}
	default:
		// ??? protoc violation
	}

	return nil, false
}

func (qr *ProxyQrouter) routeWithRules(ctx context.Context, stmt lyx.Node, sph session.SessionParamsHolder) (routingstate.RoutingState, error) {
	if stmt == nil {
		// empty statement
		return routingstate.RandomMatchState{}, nil
	}

	// if route hint forces us to route on particular route, do it
	switch v := sph.RouteHint().(type) {
	case *routehint.EmptyRouteHint:
		// nothing
	case *routehint.TargetRouteHint:
		return v.State, nil
	case *routehint.ScatterRouteHint:
		// still, need to check config settings (later)
		return routingstate.MultiMatchState{}, nil
	}

	/*
	* Currently, deparse only first query from multi-statement query msg (Enhance)
	 */

	/* TODO: delay this until step 2. */

	meta := NewRoutingMetadataContext()

	tsa := config.TargetSessionAttrsAny

	/*
	 * Step 1: traverse query tree and deparse mapping from
	 * columns to their values (either constant or expression).
	 * Note that exact (routing) value of (sharding) column may not be
	 * known after this phase, as it can be Parse Step of Extended proto.
	 */

	switch node := stmt.(type) {

	/* TDB: comments? */

	case *lyx.VariableSetStmt:
		/* TBD: maybe skip all set stmts? */
		/*
		 * SET x = y etc., do not dispatch any statement to shards, just process this in router
		 */
		return routingstate.RandomMatchState{}, nil

	case *lyx.VariableShowStmt:
		/*
		 if we want to reroute to execute this stmt, route to random shard
		 XXX: support intelegent show support, without direct query dispatch
		*/
		return routingstate.RandomMatchState{}, nil

	// XXX: need alter table which renames sharding column to non-sharding column check
	case *lyx.CreateTable:
		if val := sph.AutoDistribution(); val != "" {

			switch q := node.TableRv.(type) {
			case *lyx.RangeVar:

				/* pre-attach relation to its distribution
				 * sic! this is not transactional not abortable
				 */
				if err := qr.mgr.AlterDistributionAttach(ctx, val, []*distributions.DistributedRelation{
					{
						Name: q.RelationName,
						DistributionKey: []distributions.DistributionKeyEntry{
							{
								Column: sph.DistributionKey(),
								/* support hash function here */
							},
						},
					},
				}); err != nil {
					return nil, err
				}
			}
		}
		/*
		 * Disallow to create table which does not contain any sharding column
		 */
		if err := qr.CheckTableIsRoutable(ctx, node); err != nil {
			return nil, err
		}
		return routingstate.DDLState{}, nil
	case *lyx.Vacuum:
		/* Send vacuum to each shard */
		return routingstate.DDLState{}, nil
	case *lyx.Analyze:
		/* Send vacuum to each shard */
		return routingstate.DDLState{}, nil
	case *lyx.Cluster:
		/* Send vacuum to each shard */
		return routingstate.DDLState{}, nil
	case *lyx.Index:
		/*
		 * Disallow to index on table which does not contain any sharding column
		 */
		// XXX: do it
		return routingstate.DDLState{}, nil

	case *lyx.Alter, *lyx.Drop, *lyx.Truncate:
		// support simple ddl commands, route them to every chard
		// this is not fully ACID (not atomic at least)
		return routingstate.DDLState{}, nil
		/*
			 case *pgquery.Node_DropdbStmt, *pgquery.Node_DropRoleStmt:
				 // forbid under separate setting
				 return MultiMatchState{}, nil
		*/
	case *lyx.CreateRole, *lyx.CreateDatabase:
		// forbid under separate setting
		return routingstate.DDLState{}, nil
	case *lyx.Insert:
		err := qr.deparseShardingMapping(ctx, stmt, meta)
		if err != nil {
			return nil, err
		}
	case *lyx.Select:
		/* We cannot route SQL statements without a FROM clause. However, there are a few cases to consider. */
		if len(node.FromClause) == 0 && (node.LArg == nil || node.RArg == nil) {
			for _, expr := range node.TargetList {
				switch e := expr.(type) {
				/* Special cases for SELECT current_schema(), SELECT set_config(...), and SELECT pg_is_in_recovery() */
				case *lyx.FuncApplication:
					if e.Name == "current_schema" || e.Name == "set_config" || e.Name == "pg_is_in_recovery" {
						return routingstate.RandomMatchState{}, nil
					}
				/* Expression like SELECT 1, SELECT 'a', SELECT 1.0, SELECT true, SELECT false */
				case *lyx.AExprIConst, *lyx.AExprSConst, *lyx.AExprNConst, *lyx.AExprBConst:
					return routingstate.RandomMatchState{}, nil

				/* Special case for SELECT current_schema */
				case *lyx.ColumnRef:
					if e.ColName == "current_schema" {
						return routingstate.RandomMatchState{}, nil
					}
				}
			}
		} else {
			/* Deparse populates FromClause info, so do recurse into both branches if possible */
			if node.LArg != nil {
				if err := qr.deparseShardingMapping(ctx, node.LArg, meta); err != nil {
					return nil, err
				}
			}
			if node.RArg != nil {
				if err := qr.deparseShardingMapping(ctx, node.RArg, meta); err != nil {
					return nil, err
				}
			}
			if node.LArg == nil && node.RArg == nil {
				/* SELECT stmts, which would be routed with their WHERE clause */
				if err := qr.deparseShardingMapping(ctx, stmt, meta); err != nil {
					return nil, err
				}
			}
		}

		/*
		 *  Sometimes we have problems with some cases. For example, if a client
		 *  tries to access information schema AND other relation in same TX.
		 *  We are unable to serve this properly.
		 *  But if this is a catalog-only query, we can route it to any shard.
		 */
		hasInfSchema, onlyCatalog, anyCatalog, hasOtherSchema := false, true, false, false

		for rqfn := range meta.rels {
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
			return routingstate.RandomMatchState{}, nil
		}
		if hasInfSchema && hasOtherSchema {
			return nil, ErrInformationSchemaCombinedQuery
		}
		if hasInfSchema {
			return routingstate.RandomMatchState{}, nil
		}

	case *lyx.Delete, *lyx.Update:
		// UPDATE and/or DELETE, COPY stmts, which
		// would be routed with their WHERE clause
		err := qr.deparseShardingMapping(ctx, stmt, meta)
		if err != nil {
			return nil, err
		}
	case *lyx.Copy:
		return routingstate.CopyState{}, nil
	default:
		spqrlog.Zero.Debug().Interface("statement", stmt).Msg("proxy-routing message to all shards")
	}

	/*
	 * Step 2: traverse all aggregated relation distribution tuples and route on them.
	 */

	paramsFormatCodes := sph.BindParamFormatCodes()
	var queryParamsFormatCodes []int16

	paramsLen := len(sph.BindParams())

	// https://github.com/postgres/postgres/blob/master/src/backend/tcop/pquery.c#L635-L658
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

	var route routingstate.RoutingState
	route = nil
	var routeErr error
	for rfqn := range meta.rels {
		// TODO: check by whole RFQN
		ds, err := meta.GetRelationDistribution(ctx, qr.Mgr(), rfqn)
		if err != nil {
			return nil, err
		} else if ds.Id == distributions.REPLICATED {
			routingstate.Combine(route, routingstate.ReferenceRelationState{})
			continue
		}

		krs, err := qr.mgr.ListKeyRanges(ctx, ds.Id)
		if err != nil {
			return nil, err
		}

		relation, exists := ds.Relations[rfqn.RelationName]
		if !exists {
			return nil, fmt.Errorf("relation %s not found in distribution %s", rfqn.RelationName, ds.Id)
		}

		ok := true
		compositeKey := make([]interface{}, len(relation.DistributionKey))

		// TODO: multi-column routing. This works only for one-dim routing
		for i := range len(relation.DistributionKey) {
			hf, err := hashfunction.HashFunctionByName(relation.DistributionKey[i].HashFunction)
			if err != nil {
				ok = false
				spqrlog.Zero.Debug().Err(err).Msg("failed to resolve hash function")
				break
			}

			col := relation.DistributionKey[i].Column

			vals, valOk := qr.resolveValue(meta, rfqn, col, sph.BindParams(), queryParamsFormatCodes)

			if !valOk {
				ok = false
				break
			}

			for i, val := range vals {
				compositeKey[i], err = hashfunction.ApplyHashFunction(val, ds.ColTypes[i], hf)
				spqrlog.Zero.Debug().Interface("key", val).Interface("hashed key", compositeKey[i]).Msg("applying hash function on key")

				if err != nil {
					spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
					ok = false
					break
				}
				// only works for one value
				break
			}
		}

		if !ok {
			// skip this relation
			continue
		}

		currroute, err := qr.DeparseKeyWithRangesInternal(ctx, compositeKey, krs)
		if err != nil {
			routeErr = err
			spqrlog.Zero.Debug().Err(routeErr).Msg("temporarily skip the route error")
			continue
		}

		spqrlog.Zero.Debug().
			Interface("currroute", currroute).
			Str("table", rfqn.RelationName).
			Msg("calculated route for table/cols")

		route = routingstate.Combine(route, routingstate.ShardMatchState{
			Route:              currroute,
			TargetSessionAttrs: tsa,
		})
	}

	if route == nil && routeErr != nil {
		return nil, routeErr
	}

	// set up this variable if not yet
	if route == nil {
		route = routingstate.MultiMatchState{}
	}

	return route, nil
}

// TODO : unit tests
func (qr *ProxyQrouter) Route(ctx context.Context, stmt lyx.Node, sph session.SessionParamsHolder) (routingstate.RoutingState, error) {
	route, err := qr.routeWithRules(ctx, stmt, sph)
	if err != nil {
		return nil, err
	}

	switch v := route.(type) {
	case routingstate.ShardMatchState:
		return v, nil
	case routingstate.RandomMatchState:
		return v, nil
	case routingstate.ReferenceRelationState:
		return v, nil
	case routingstate.DDLState:
		return v, nil
	case routingstate.CopyState:
		/* temporary */
		return routingstate.MultiMatchState{}, nil
	case routingstate.MultiMatchState:
		/*
		* Here we have a chance for advanced multi-shard query processing.
		* Try to build distributed plan, else scatter-out.
		 */
		switch sph.DefaultRouteBehaviour() {
		case "BLOCK":
			return routingstate.SkipRoutingState{}, spqrerror.NewByCode(spqrerror.SPQR_NO_DATASHARD)
		case "ALLOW":
			fallthrough
		default:
			/* TODO: config options for this */

			if qr.cfg.EnhancedMultiShardProcessing {
				v.DistributedPlan = plan.PlanDistributedQuery(ctx, stmt)
			}

			return v, nil
		}
	}
	return routingstate.SkipRoutingState{}, nil
}
