package qrouter

import (
	"context"
	"fmt"
	"math"

	"github.com/pg-sharding/lyx/lyx"
	ds "github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
)

type SolveResult struct {
	Result         bool
	CauseNotSolved string
}

func NotSolved(cause string) *SolveResult {
	return &SolveResult{Result: false, CauseNotSolved: cause}
}
func Solved() *SolveResult {
	return &SolveResult{Result: true}
}

func Combine(first *SolveResult, second *SolveResult, operation string) *SolveResult {

	if operation == "and" && first.Result && second.Result {
		return first
	} else if operation == "or" && (first.Result || second.Result) {
		return Solved()
	} else {
		return NotSolved(first.CauseNotSolved + "|" + second.CauseNotSolved)
	}
}

func solveLeaf(expr lyx.AExprOp,
	krCurrent *kr.KeyRange,
	krNext *kr.KeyRange,
	relation *ds.DistributedRelation,
	ds *ds.Distribution,
) (*SolveResult, error) {
	rightBorderOfCurrent := &kr.KeyRangeBound{int64(math.MaxInt64)}
	if krNext != nil {
		rightBorderOfCurrent = &kr.KeyRangeBound{int64(krNext.LowerBound[0].(int64) - 1)} //быстро костыльно
	}
	if relation.ReplicatedRelation {
		return nil, fmt.Errorf("replicated relation is not implemented to solve")
	}
	if expr.Left == nil {
		return nil, fmt.Errorf("can't solve expression (case 1)")
	}
	var columnExpr *lyx.ColumnRef
	var boundFromExpression *kr.KeyRangeBound
	op := expr.Op
	if column, ok := expr.Left.(*lyx.ColumnRef); ok {
		columnExpr = column
	}
	if constValue, ok := expr.Right.(*lyx.AExprIConst); ok {
		boundFromExpression = &kr.KeyRangeBound{int64(constValue.Value)}
	}
	if columnExpr == nil || boundFromExpression == nil {
		return nil, fmt.Errorf("not all parts of expression are found")
	}
	switch op {
	case ">":
		if kr.CmpRangesLess(*boundFromExpression, *rightBorderOfCurrent, ds.ColTypes) {
			return Solved(), nil
		} else {
			return NotSolved("the operator '>' solved the game"), nil
		}
	case "<":
		if kr.CmpRangesLess(krCurrent.LowerBound, *boundFromExpression, ds.ColTypes) {
			return Solved(), nil
		} else {
			return NotSolved("the operator '>' solved the game"), nil
		}
	default:
		return nil, fmt.Errorf("operation %s is not implemented", op)
	}
}

func solveInternal(whereStmt lyx.AExprOp,
	krCurrent *kr.KeyRange,
	krNext *kr.KeyRange,
	relation *ds.DistributedRelation,
	ds *ds.Distribution,
) (*SolveResult, error) {
	_, okCol := whereStmt.Left.(*lyx.ColumnRef)
	_, okVal := whereStmt.Right.(*lyx.AExprIConst)
	//leaf
	if okCol && okVal {
		return solveLeaf(whereStmt, krCurrent, krNext, relation, ds)
	}
	//not leaf
	if whereStmt.Op != "and" && whereStmt.Op != "or" {
		return nil, fmt.Errorf("operation %s not implemented for where solver", whereStmt.Op)
	}

	leftSolved := Solved()
	rightSolved := Solved()
	var err error
	if left, okLeft := whereStmt.Left.(*lyx.AExprOp); okLeft {
		leftSolved, err = solveInternal(*left, krCurrent, krNext, relation, ds)
		if err != nil {
			return nil, err
		}
	}
	if right, okRight := whereStmt.Right.(*lyx.AExprOp); okRight {
		rightSolved, err = solveInternal(*right, krCurrent, krNext, relation, ds)
		if err != nil {
			return nil, err
		}
	}
	return Combine(leftSolved, rightSolved, whereStmt.Op), nil
}

func solveKeyRange(whereStmt lyx.AExprOp,
	krCurrent *kr.KeyRange,
	krNext *kr.KeyRange,
	relation *ds.DistributedRelation,
	ds *ds.Distribution,
) (*SolveResult, error) {
	if len(ds.ColTypes) != 1 {
		return nil, fmt.Errorf("solver is implemented for distribution with single val key")
	}
	if ds.ColTypes[0] != "integer" {
		return nil, fmt.Errorf("solver is implemented for integer val")
	}
	if krNext != nil && !kr.CmpRangesLess(krCurrent.LowerBound, krNext.LowerBound, ds.ColTypes) {
		return nil, fmt.Errorf("invalid key range pair in solver: %s, %s", krCurrent.ID, krNext.ID)
	}
	if relation.ReplicatedRelation {
		return nil, fmt.Errorf("replicated relation is not implemented to solve")
	}
	return solveInternal(whereStmt, krCurrent, krNext, relation, ds)
}

func SolveShards(whereStmt lyx.AExprOp,
	krs []*kr.KeyRange,
	relation *ds.DistributedRelation,
	ds *ds.Distribution,
) ([]string, error) {
	if len(krs) == 0 {
		return nil, fmt.Errorf("not found key ranges for relation %s", relation.Name)
	}
	shards := make(map[string]struct{})
	for i := range krs[:len(krs)-1] {
		if res, err := solveKeyRange(whereStmt, krs[i], krs[i+1], relation, ds); err != nil {
			return nil, err
		} else {
			if res.Result {
				shards[krs[i].ShardID] = struct{}{}
			}
		}
	}
	if res, err := solveKeyRange(whereStmt, krs[len(krs)-1], nil, relation, ds); err != nil {
		return nil, err
	} else {
		if res.Result {
			shards[krs[len(krs)-1].ShardID] = struct{}{}
		}
	}
	result := make([]string, 0, len(shards))
	for i := range shards {
		result = append(result, i)
	}
	return result, nil
}

func prepareDataForSolver(ctx context.Context,
	rm *rmeta.RoutingMetadataContext, stmt *lyx.Select) (*ds.DistributedRelation,
	*ds.Distribution, []*kr.KeyRange, error) {
	if len(stmt.FromClause) != 1 {
		return nil, nil, nil, fmt.Errorf("prepare data for solver: not implemented (case 0)")
	}
	if relationData, ok := stmt.FromClause[0].(*lyx.RangeVar); !ok {
		return nil, nil, nil, fmt.Errorf("prepare data for solver: relation is not resolved")
	} else {
		qualName := rfqn.RelationFQNFromRangeRangeVar(relationData)
		var distribution *ds.Distribution
		var relation *ds.DistributedRelation
		var krList []*kr.KeyRange
		var err error
		if distribution, err = rm.GetRelationDistribution(ctx, qualName); err != nil {
			return nil, nil, nil, err
		}

		if relation, ok = distribution.Relations[qualName.String()]; !ok {
			return nil, nil, nil, fmt.Errorf("not found relation by rfqn: %#v", qualName)
		}
		if krList, err = rm.Mgr.ListKeyRanges(ctx, distribution.Id); err != nil {
			return nil, nil, nil, err
		}
		return relation, distribution, krList, nil
	}
}
