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

func PlanUtility(ctx context.Context, rm *rmeta.RoutingMetadataContext, stmt lyx.Node) (plan.Plan, error) {

	switch node := stmt.(type) {

	/* TDB: comments? */

	case *lyx.VariableSetStmt:
		/* TBD: maybe skip all set stmts? */
		/*
		 * SET x = y etc., do not dispatch any statement to shards, just process this in router
		 */
		return &plan.RandomDispatchPlan{}, nil

	case *lyx.VariableShowStmt:
		/*
		 if we want to reroute to execute this stmt, route to random shard
		 XXX: support intelligent show support, without direct query dispatch
		*/
		return &plan.RandomDispatchPlan{}, nil

	// XXX: need alter table which renames sharding column to non-sharding column check
	case *lyx.CreateSchema:
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil

	case *lyx.DefineStmt:
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil

	case *lyx.CreateExtension:
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil
	case *lyx.Grant:
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil
	case *lyx.CreateTable:
		ds, err := PlanCreateTable(ctx, rm, node)
		if err != nil {
			return nil, err
		}
		/*
		 * Disallow to create table which does not contain any sharding column
		 */
		if err := CheckRelationIsRoutable(ctx, rm.Mgr, node); err != nil {
			return nil, err
		}
		return ds, nil
	case *lyx.VacuumStmt:
		/* Send vacuum to each shard */
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil

	case *lyx.Cluster:
		/* Send vacuum to each shard */
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil
	case *lyx.CreateIndex:
		/*
		 * Disallow to index on table which does not contain any sharding column
		 */
		// XXX: do it
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil

	case *lyx.Alter, *lyx.Drop, *lyx.Truncate:
		// support simple ddl commands, route them to every chard
		// this is not fully ACID (not atomic at least)
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil
		/*
			 case *pgquery.Node_DropdbStmt, *pgquery.Node_DropRoleStmt:
				 // forbid under separate setting
				 return MultiMatchState{}, nil
		*/
	case *lyx.CreateRole, *lyx.CreateDatabase:
		// forbid under separate setting
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil
	case *lyx.Delete, *lyx.Update, *lyx.Select, *lyx.Insert, *lyx.ValueClause, *lyx.ExplainStmt:
		/* do not bother with those */
		return nil, nil
	case *lyx.Copy:
		return &plan.CopyPlan{}, nil
	default:
		return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
	}
}

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

// CheckRelationIsRoutable Given table create statement,
// check if it is routable with some distribution rule
// TODO : unit tests
func CheckRelationIsRoutable(ctx context.Context, mgr meta.EntityMgr, node *lyx.CreateTable) error {
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

		/* maybe this is actually reverse index? */

		/* TODO: add proper method to mgr */
		iis, err := mgr.ListUniqueIndexes(ctx)
		if err != nil {
			return err
		}
		for _, is := range iis {
			spqrlog.Zero.Debug().Str("name", is.RelationName.String()).Msg("check index")
			if is.ID == relname.String() {
				/* this is an index table */

				/* XXX: TODO - check columns */
				return nil
			}
		}

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
	/* Collect column entries list from create statement */
	for _, elt := range node.TableElts {
		// hashing function name unneeded for column matching purpose
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

	return fmt.Errorf("create table stmt ignored: no matching distribution found")
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
