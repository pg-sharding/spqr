package rmeta

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
)

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
	Exprs     map[rfqn.RelationFQN]map[string][]interface{}
	ParamRefs map[rfqn.RelationFQN]map[string][]int

	// cached CTE names
	CteNames map[string]struct{}

	// needed to parse
	// SELECT * FROM t1 a where a.i = 1
	// rarg:{range_var:{relname:"t2" inh:true relpersistence:"p" alias:{aliasname:"b"}
	TableAliases map[string]rfqn.RelationFQN

	SPH session.SessionParamsHolder

	Mgr meta.EntityMgr

	Distributions map[rfqn.RelationFQN]*distributions.Distribution
}

func NewRoutingMetadataContext(sph session.SessionParamsHolder, mgr meta.EntityMgr) *RoutingMetadataContext {
	return &RoutingMetadataContext{
		Rels:          map[rfqn.RelationFQN]struct{}{},
		CteNames:      map[string]struct{}{},
		TableAliases:  map[string]rfqn.RelationFQN{},
		Exprs:         map[rfqn.RelationFQN]map[string][]interface{}{},
		ParamRefs:     map[rfqn.RelationFQN]map[string][]int{},
		Distributions: map[rfqn.RelationFQN]*distributions.Distribution{},
		SPH:           sph,
		Mgr:           mgr,
	}
}

var CatalogDistribution = distributions.Distribution{
	Relations: nil,
	Id:        distributions.REPLICATED,
	ColTypes:  nil,
}

func (rm *RoutingMetadataContext) GetRelationDistribution(ctx context.Context, mgr meta.EntityMgr, resolvedRelation rfqn.RelationFQN) (*distributions.Distribution, error) {
	if res, ok := rm.Distributions[resolvedRelation]; ok {
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

	rm.Distributions[resolvedRelation] = ds
	return ds, nil
}

func (rm *RoutingMetadataContext) RFQNIsCTE(resolvedRelation rfqn.RelationFQN) bool {
	_, ok := rm.CteNames[resolvedRelation.RelationName]
	return len(resolvedRelation.SchemaName) == 0 && ok
}

// TODO : unit tests
func (rm *RoutingMetadataContext) RecordConstExpr(resolvedRelation rfqn.RelationFQN, colname string, expr interface{}) error {
	if rm.RFQNIsCTE(resolvedRelation) {
		// CTE, skip
		return nil
	}
	rm.Rels[resolvedRelation] = struct{}{}
	if _, ok := rm.Exprs[resolvedRelation]; !ok {
		rm.Exprs[resolvedRelation] = map[string][]interface{}{}
	}
	if _, ok := rm.Exprs[resolvedRelation][colname]; !ok {
		rm.Exprs[resolvedRelation][colname] = make([]interface{}, 0)
	}
	rm.Exprs[resolvedRelation][colname] = append(rm.Exprs[resolvedRelation][colname], expr)
	return nil
}

func (routingMeta *RoutingMetadataContext) RecordParamRefExpr(resolvedRelation rfqn.RelationFQN, colname string, ind int) error {
	if routingMeta.RFQNIsCTE(resolvedRelation) {
		// CTE, skip
		return nil
	}

	if routingMeta.Distributions[resolvedRelation].Id == distributions.REPLICATED {
		// referencr relation, skip
		return nil
	}

	routingMeta.Rels[resolvedRelation] = struct{}{}
	if _, ok := routingMeta.ParamRefs[resolvedRelation]; !ok {
		routingMeta.ParamRefs[resolvedRelation] = map[string][]int{}
	}
	if _, ok := routingMeta.ParamRefs[resolvedRelation][colname]; !ok {
		routingMeta.ParamRefs[resolvedRelation][colname] = make([]int, 0)
	}
	routingMeta.ParamRefs[resolvedRelation][colname] = append(routingMeta.ParamRefs[resolvedRelation][colname], ind)
	return nil
}

// TODO : unit tests
func (rm *RoutingMetadataContext) ResolveRelationByAlias(alias string) (rfqn.RelationFQN, error) {
	if _, ok := rm.Rels[rfqn.RelationFQN{RelationName: alias}]; ok {
		return rfqn.RelationFQN{RelationName: alias}, nil
	}
	if resolvedRelation, ok := rm.TableAliases[alias]; ok {
		// TBD: postpone routing from here to root of parsing tree
		return resolvedRelation, nil
	} else {
		// TBD: postpone routing from here to root of parsing tree
		if len(rm.Rels) != 1 {
			// ambiguity in column aliasing
			return rfqn.RelationFQN{}, rerrors.ErrComplexQuery
		}
		for tbl := range rm.Rels {
			resolvedRelation = tbl
		}
		return resolvedRelation, nil
	}
}
