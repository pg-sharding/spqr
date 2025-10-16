package rmeta

import (
	"context"
	"fmt"
	"strconv"

	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"

	"github.com/pg-sharding/lyx/lyx"
)

type AuxValuesKey struct {
	CTEName   string
	ValueName string
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
	Rels      map[rfqn.RelationFQN]struct{}
	Exprs     map[rfqn.RelationFQN]map[string][]any
	ParamRefs map[rfqn.RelationFQN]map[string][]int

	// cached CTE names
	CteNames map[string]struct{}

	// needed to parse
	// SELECT * FROM t1 a where a.i = 1
	// rarg:{range_var:{relname:"t2" inh:true relpersistence:"p" alias:{aliasname:"b"}
	TableAliases map[string]rfqn.RelationFQN
	CTEAliases   map[string]string

	SPH session.SessionParamsHolder

	CSM connmgr.ConnectionStatMgr
	Mgr meta.EntityMgr

	AuxValues map[AuxValuesKey][]lyx.Node

	Distributions map[rfqn.RelationFQN]*distributions.Distribution
}

func NewRoutingMetadataContext(sph session.SessionParamsHolder, csm connmgr.ConnectionStatMgr, mgr meta.EntityMgr) *RoutingMetadataContext {
	return &RoutingMetadataContext{
		Rels:          map[rfqn.RelationFQN]struct{}{},
		CteNames:      map[string]struct{}{},
		TableAliases:  map[string]rfqn.RelationFQN{},
		CTEAliases:    map[string]string{},
		Exprs:         map[rfqn.RelationFQN]map[string][]any{},
		ParamRefs:     map[rfqn.RelationFQN]map[string][]int{},
		Distributions: map[rfqn.RelationFQN]*distributions.Distribution{},
		AuxValues:     map[AuxValuesKey][]lyx.Node{},
		SPH:           sph,
		CSM:           csm,
		Mgr:           mgr,
	}
}

var CatalogDistribution = distributions.Distribution{
	Relations: nil,
	Id:        distributions.REPLICATED,
	ColTypes:  nil,
}

func IsRelationCatalog(resolvedRelation *rfqn.RelationFQN) bool {
	return len(resolvedRelation.RelationName) >= 3 && resolvedRelation.RelationName[0:3] == "pg_"
}

func (rm *RoutingMetadataContext) IsReferenceRelation(ctx context.Context, q *lyx.RangeVar) (bool, error) {
	qualName := rfqn.RelationFQNFromRangeRangeVar(q)

	ds, err := rm.GetRelationDistribution(ctx, qualName)
	if err != nil {
		return false, err
	}
	return ds.Id == distributions.REPLICATED, nil
}

func (rm *RoutingMetadataContext) RecordAuxExpr(name string, value string, v lyx.Node) {
	k := AuxValuesKey{
		CTEName:   name,
		ValueName: value,
	}
	vals := rm.AuxValues[k]
	vals = append(vals, v)
	rm.AuxValues[k] = vals
}

func (rm *RoutingMetadataContext) ResolveValue(rfqn *rfqn.RelationFQN, col string, paramResCodes []int16) ([]any, error) {
	/* explicit assignment in query */
	if vals, ok := rm.Exprs[*rfqn][col]; ok {
		return vals, nil
	}

	/* else get parameter from bind query */

	inds, ok := rm.ParamRefs[*rfqn][col]
	if !ok {
		return nil, plan.ErrResolvingValue
	}

	off, tp := rm.GetDistributionKeyOffsetType(rfqn, col)
	if off == -1 {
		// column not from distr key
		return nil, plan.ErrResolvingValue
	}

	// TODO: switch column type here
	// only works for one value
	ind := inds[0]
	if len(paramResCodes) < ind {
		return nil, plan.ErrResolvingValue
	}
	fc := paramResCodes[ind]

	singleVal, err := plan.ParseResolveParamValue(fc, ind, tp, rm.SPH.BindParams())

	return []any{singleVal}, err
}

func (rm *RoutingMetadataContext) AuxExprByColref(cf *lyx.ColumnRef) []lyx.Node {
	searchKey := cf.TableAlias
	if fullName, ok := rm.CTEAliases[cf.TableAlias]; ok {
		searchKey = fullName
	}

	k := AuxValuesKey{
		CTEName:   searchKey,
		ValueName: cf.ColName,
	}
	return rm.AuxValues[k]
}

func (rm *RoutingMetadataContext) GetRelationDistribution(ctx context.Context, resolvedRelation *rfqn.RelationFQN) (*distributions.Distribution, error) {
	if res, ok := rm.Distributions[*resolvedRelation]; ok {
		return res, nil
	}

	if IsRelationCatalog(resolvedRelation) {
		return &CatalogDistribution, nil
	}

	if resolvedRelation.SchemaName == "information_schema" {
		return &CatalogDistribution, nil
	}

	ds, err := rm.Mgr.GetRelationDistribution(ctx, resolvedRelation)

	if err != nil {
		return nil, err
	}

	rm.Distributions[*resolvedRelation] = ds
	return ds, nil
}

func (rm *RoutingMetadataContext) RFQNIsCTE(resolvedRelation *rfqn.RelationFQN) bool {
	_, ok := rm.CteNames[resolvedRelation.RelationName]
	return len(resolvedRelation.SchemaName) == 0 && ok
}

// TODO : unit tests
func (rm *RoutingMetadataContext) RecordConstExpr(resolvedRelation *rfqn.RelationFQN, colname string, expr any) error {
	rm.Rels[*resolvedRelation] = struct{}{}
	if _, ok := rm.Exprs[*resolvedRelation]; !ok {
		rm.Exprs[*resolvedRelation] = map[string][]any{}
	}
	if _, ok := rm.Exprs[*resolvedRelation][colname]; !ok {
		rm.Exprs[*resolvedRelation][colname] = make([]any, 0)
	}
	rm.Exprs[*resolvedRelation][colname] = append(rm.Exprs[*resolvedRelation][colname], expr)
	return nil
}

func (routingMeta *RoutingMetadataContext) RecordParamRefExpr(resolvedRelation *rfqn.RelationFQN, colname string, ind int) error {
	routingMeta.Rels[*resolvedRelation] = struct{}{}
	if _, ok := routingMeta.ParamRefs[*resolvedRelation]; !ok {
		routingMeta.ParamRefs[*resolvedRelation] = map[string][]int{}
	}
	if _, ok := routingMeta.ParamRefs[*resolvedRelation][colname]; !ok {
		routingMeta.ParamRefs[*resolvedRelation][colname] = make([]int, 0)
	}
	routingMeta.ParamRefs[*resolvedRelation][colname] = append(routingMeta.ParamRefs[*resolvedRelation][colname], ind)
	return nil
}

// TODO : unit tests
func (rm *RoutingMetadataContext) ResolveRelationByAlias(alias string) (*rfqn.RelationFQN, error) {
	if _, ok := rm.Rels[rfqn.RelationFQN{RelationName: alias}]; ok {
		return &rfqn.RelationFQN{RelationName: alias}, nil
	}
	if resolvedRelation, ok := rm.TableAliases[alias]; ok {
		// TBD: postpone routing from here to root of parsing tree
		return &resolvedRelation, nil
	} else {
		// TBD: postpone routing from here to root of parsing tree
		if len(rm.Rels) != 1 {
			// ambiguity in column aliasing
			return nil, rerrors.ErrComplexQuery
		}
		for tbl := range rm.Rels {
			resolvedRelation = tbl
		}
		return &resolvedRelation, nil
	}
}

// TODO : unit tests
func (rm *RoutingMetadataContext) DeparseKeyWithRangesInternal(_ context.Context, key []any, krs []*kr.KeyRange) (kr.ShardKey, error) {
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
		if err := rm.Mgr.ShareKeyRange(matchedKrkey.ID); err != nil {
			return kr.ShardKey{}, err
		}
		return kr.ShardKey{Name: matchedKrkey.ShardID}, nil
	}
	spqrlog.Zero.Debug().Msg("failed to match key with ranges")

	return kr.ShardKey{}, fmt.Errorf("failed to match key with ranges")
}

func (rm *RoutingMetadataContext) ResolveRouteHint(ctx context.Context) (plan.Plan, error) {
	if rm.SPH.ScatterQuery() {
		return &plan.ScatterPlan{
			Forced: true,
		}, nil
	}
	if val := rm.SPH.ShardingKey(); val != "" {
		spqrlog.Zero.Debug().Str("sharding key", val).Msg("checking hint key")

		dsId := rm.SPH.Distribution()
		if dsId == "" {
			return nil, spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "sharding key in comment without distribution")
		}

		krs, err := rm.Mgr.ListKeyRanges(ctx, dsId)
		if err != nil {
			return nil, err
		}

		distrib, err := rm.Mgr.GetDistribution(ctx, dsId)
		if err != nil {
			return nil, err
		}

		if len(distrib.ColTypes) > 1 {
			return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "multi-column sharding key in comment no supported yet")
		}

		// TODO: fix this
		compositeKey, err := kr.KeyRangeBoundFromStrings(distrib.ColTypes, []string{val})

		if err != nil {
			return nil, err
		}

		dRel := rm.SPH.DistributedRelation()

		hf := hashfunction.HashFunctionIdent

		if dRel != "" {
			relName, err := rfqn.ParseFQN(dRel)
			if err != nil {
				return nil, err
			}
			r, ok := distrib.TryGetRelation(relName)
			if ok {
				hf, err = hashfunction.HashFunctionByName(r.DistributionKey[0].HashFunction)
				if err != nil {
					return nil, err
				}
			}
		} else {
			first := true
			for _, dr := range distrib.Relations {
				hfLocal, err := hashfunction.HashFunctionByName(dr.DistributionKey[0].HashFunction)
				if err != nil {
					return nil, err
				}
				if first {
					hf = hfLocal
				} else {
					if hf != hfLocal {
						return nil, fmt.Errorf("failed to resolve hint hash function")
					}
				}
			}
		}

		compositeKey[0], err = hashfunction.ApplyHashFunction(compositeKey[0], distrib.ColTypes[0], hf)
		if err != nil {
			return nil, err
		}

		ds, err := rm.DeparseKeyWithRangesInternal(ctx, compositeKey, krs)
		if err != nil {
			return nil, err
		}
		return &plan.ShardDispatchPlan{
			ExecTarget: ds,
		}, nil
	}

	return nil, nil
}

func (rm *RoutingMetadataContext) GetDistributionKeyOffsetType(resolvedRelation *rfqn.RelationFQN, colname string) (int, string) {
	/* do not process non-distributed relations or columns not from relation distribution key */

	ds, err := rm.GetRelationDistribution(context.TODO(), resolvedRelation)
	if err != nil {
		return -1, ""
	} else if ds.Id == distributions.REPLICATED {
		return -1, ""
	}
	// TODO: optimize
	relation, exists := ds.TryGetRelation(resolvedRelation)
	if !exists {
		return -1, ""
	}
	for ind, c := range relation.DistributionKey {
		if c.Column == colname {
			return ind, ds.ColTypes[ind]
		}
		for _, cf := range c.Expr.ColRefs {
			if cf.ColName == colname {
				return ind, cf.ColType
			}
		}
	}
	return -1, ""
}

type ParamRef struct {
	Indx int
}

func ParseExprValue(tp string, expr lyx.Node) (any, error) {
	switch right := expr.(type) {
	case *lyx.ParamRef:
		return ParamRef{Indx: right.Number - 1}, nil
	case *lyx.AExprSConst:
		switch tp {
		case qdb.ColumnTypeUUID:
			fallthrough
		case qdb.ColumnTypeVarcharDeprecated:
			fallthrough
		case qdb.ColumnTypeVarcharHashed:
			fallthrough
		case qdb.ColumnTypeVarchar:
			return right.Value, nil
		case qdb.ColumnTypeInteger:
			num, err := strconv.ParseInt(right.Value, 10, 64)
			if err != nil {
				return nil, err
			}
			return num, nil
		case qdb.ColumnTypeUinteger:
			num, err := strconv.ParseUint(right.Value, 10, 64)
			if err != nil {
				return nil, err
			}
			return num, nil
		default:
			return nil, fmt.Errorf("incorrect key-offset type for AExprSConst expression: %s", tp)
		}
	case *lyx.AExprIConst:
		switch tp {
		case qdb.ColumnTypeUUID:
			return nil, fmt.Errorf("uuid type is not supported for AExprIConst expression")
		case qdb.ColumnTypeVarcharDeprecated:
			fallthrough
		case qdb.ColumnTypeVarcharHashed:
			fallthrough
		case qdb.ColumnTypeVarchar:
			return nil, fmt.Errorf("varchar type is not supported for AExprIConst expression")
		case qdb.ColumnTypeInteger:
			return int64(right.Value), nil
		case qdb.ColumnTypeUinteger:
			return uint64(right.Value), nil
		default:
			return nil, fmt.Errorf("incorrect key-offset type for AExprIConst expression: %s", tp)
		}
	default:
		return nil, fmt.Errorf("expression is not const")
	}
}

func (rm *RoutingMetadataContext) ProcessSingleExpr(resolvedRelation *rfqn.RelationFQN, tp string, colname string, expr lyx.Node) error {

	if rm.RFQNIsCTE(resolvedRelation) {
		// CTE, skip
		return nil
	}

	if rm.Distributions[*resolvedRelation].Id == distributions.REPLICATED {
		// reference relation, skip
		return nil
	}

	v, err := ParseExprValue(tp, expr)
	if err != nil {
		return err
	}

	switch q := v.(type) {
	case ParamRef:
		return rm.RecordParamRefExpr(resolvedRelation, colname, q.Indx)
	default:
		return rm.RecordConstExpr(resolvedRelation, colname, q)
	}
}
