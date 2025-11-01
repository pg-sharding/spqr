package planner

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
)

func ProcessInsertFromSelectOffsets(
	ctx context.Context, stmt *lyx.Insert, meta *rmeta.RoutingMetadataContext) (map[string]int, *rfqn.RelationFQN, error) {
	insertCols := stmt.Columns

	spqrlog.Zero.Debug().
		Strs("insert columns", insertCols).
		Msg("deparsed insert statement columns")

	var curr_rfqn *rfqn.RelationFQN

	switch q := stmt.TableRef.(type) {
	case *lyx.RangeVar:

		spqrlog.Zero.Debug().
			Str("relname", q.RelationName).
			Str("schemaname", q.SchemaName).
			Msg("deparsed insert statement table ref")

		curr_rfqn = rfqn.RelationFQNFromRangeRangeVar(q)

		insertColsPos := map[string]int{}
		for i, c := range insertCols {
			insertColsPos[c] = i
		}

		return insertColsPos, curr_rfqn, nil
	default:
		return nil, nil, rerrors.ErrComplexQuery
	}
}

func SelectRandomDispatchPlan(routes []kr.ShardKey) (plan.Plan, error) {
	if len(routes) == 0 {
		return nil, fmt.Errorf("no routes configured")
	}

	r := routes[rand.Int()%len(routes)]
	return &plan.ShardDispatchPlan{
		ExecTarget: r,
	}, nil
}

// CheckTableIsRoutable Given table create statement, check if it is routable with some sharding rule
// TODO : unit tests
func CheckTableIsRoutable(ctx context.Context, mgr meta.EntityMgr, node *lyx.CreateTable) error {
	var err error
	var ds *distributions.Distribution
	var relname *rfqn.RelationFQN

	if node.PartitionOf != nil {
		switch q := node.PartitionOf.(type) {
		case *lyx.RangeVar:
			relname := rfqn.RelationFQNFromRangeRangeVar(q)
			_, err = mgr.GetRelationDistribution(ctx, relname)
			return err
		default:
			return fmt.Errorf("partition of is not a range var")
		}
	}

	switch q := node.TableRv.(type) {
	case *lyx.RangeVar:
		relname = rfqn.RelationFQNFromRangeRangeVar(q)
		ds, err = mgr.GetRelationDistribution(ctx, relname)
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
	rel, ok := ds.TryGetRelation(relname)
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "relation \"%s\" not present in distribution \"%s\" it's attached to", relname, ds.Id)
	}
	check := true
	for _, entry := range rel.DistributionKey {
		if len(entry.Column) == 0 {
			if len(entry.Expr.ColRefs) == 0 {
				return fmt.Errorf("invalid routing expression for relation")
			} else {
				for _, cf := range entry.Expr.ColRefs {
					if _, ok = entries[cf.ColName]; !ok {
						check = false
						break
					}
				}
			}
		} else {
			if _, ok = entries[entry.Column]; !ok {
				check = false
				break
			}
		}
	}
	if check {
		return nil
	}

	return fmt.Errorf("create table stmt ignored: no sharding rule columns found")
}

func ProcessRangeNode(ctx context.Context, rm *rmeta.RoutingMetadataContext, q *lyx.RangeVar) error {
	qualName := rfqn.RelationFQNFromRangeRangeVar(q)

	// CTE, skip
	if rm.RFQNIsCTE(qualName) {
		/* remember cte alias */
		rm.CTEAliases[q.Alias] = qualName.RelationName
		return nil
	}

	if _, err := rm.GetRelationDistribution(ctx, qualName); err != nil {
		return err
	}

	if _, ok := rm.Rels[*qualName]; !ok {
		rm.Rels[*qualName] = struct{}{}
	}
	if q.Alias != "" {
		/* remember table alias */
		rm.TableAliases[q.Alias] = *rfqn.RelationFQNFromRangeRangeVar(q)
	}
	return nil
}
