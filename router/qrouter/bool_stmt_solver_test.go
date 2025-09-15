package qrouter

import (
	"testing"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/stretchr/testify/assert"
)

func TestTypeCast(t *testing.T) {
	is := assert.New(t)
	query := "select * from xx where i = 1 "
	expected := &lyx.Select{
		TargetList: []lyx.Node{&lyx.AExprEmpty{}},
		FromClause: []lyx.FromClauseNode{&lyx.RangeVar{
			RelationName: "xx",
		},
		},
		Where: &lyx.AExprOp{
			Left: &lyx.ColumnRef{
				ColName:    "i",
				TableAlias: "",
			},
			Right: &lyx.AExprIConst{
				Value: 1,
			},
			Op: "=",
		},
	}
	result, err := lyx.Parse(query)

	is.NoError(err)
	is.Equal(expected, result[0])
}

func TestSolveSimple(t *testing.T) {
	is := assert.New(t)
	whereStmt := &lyx.AExprOp{
		Left: &lyx.ColumnRef{
			ColName:    "i",
			TableAlias: "",
		},
		Right: &lyx.AExprIConst{
			Value: 1,
		},
		Op: ">",
	}
	krs := []*kr.KeyRange{
		{ID: "kr1", LowerBound: []any{int64(-1)}, ColumnTypes: []string{"integer"}},
		{ID: "kr2", LowerBound: []any{int64(0)}, ColumnTypes: []string{"integer"}},
		{ID: "kr3", LowerBound: []any{int64(5)}, ColumnTypes: []string{"integer"}},
		{ID: "kr4", LowerBound: []any{int64(10)}, ColumnTypes: []string{"integer"}},
	}
	ds := &distributions.Distribution{ColTypes: []string{"integer"}}
	rel := &distributions.DistributedRelation{
		Name: "xx",
		DistributionKey: []distributions.DistributionKeyEntry{
			{Column: "i"},
		},
	}
	res, err := solveKeyRange(*whereStmt, krs[1], krs[2], rel, ds)
	is.NoError(err)
	is.Equal(*Solved(), *res)

	res, err = solveKeyRange(*whereStmt, krs[2], krs[3], rel, ds)
	is.NoError(err)
	is.Equal(*Solved(), *res)

	res, err = solveKeyRange(*whereStmt, krs[3], nil, rel, ds)
	is.NoError(err)
	is.Equal(*Solved(), *res)

	res, err = solveKeyRange(*whereStmt, krs[0], krs[1], rel, ds)
	is.NoError(err)
	is.Equal(NotSolved("the operator '>' solved the game"), res)
}

func TestSolveComplex(t *testing.T) {
	is := assert.New(t)
	whereStmt :=
		&lyx.AExprOp{
			Left: &lyx.AExprOp{
				Left: &lyx.ColumnRef{
					ColName:    "i",
					TableAlias: "",
				},
				Right: &lyx.AExprIConst{
					Value: 1,
				},
				Op: ">",
			},
			Right: &lyx.AExprOp{
				Left: &lyx.ColumnRef{
					ColName:    "i",
					TableAlias: "",
				},
				Right: &lyx.AExprIConst{
					Value: 5,
				},
				Op: "<",
			},
			Op: "and",
		}

	krs := []*kr.KeyRange{
		{ID: "kr1", LowerBound: []any{int64(-1)}, ColumnTypes: []string{"integer"}, ShardID: "sh1"},
		{ID: "kr2", LowerBound: []any{int64(0)}, ColumnTypes: []string{"integer"}, ShardID: "sh1"},
		{ID: "kr3", LowerBound: []any{int64(5)}, ColumnTypes: []string{"integer"}, ShardID: "sh2"},
		{ID: "kr4", LowerBound: []any{int64(10)}, ColumnTypes: []string{"integer"}, ShardID: "sh2"},
	}
	ds := &distributions.Distribution{ColTypes: []string{"integer"}}
	rel := &distributions.DistributedRelation{
		Name: "xx",
		DistributionKey: []distributions.DistributionKeyEntry{
			{Column: "i"},
		},
	}
	res, err := solveKeyRange(*whereStmt, krs[1], krs[2], rel, ds)
	is.NoError(err)
	is.Equal(*Solved(), *res)
}

func TestSolveShardsHappyPath(t *testing.T) {
	is := assert.New(t)
	whereStmt :=
		&lyx.AExprOp{
			Left: &lyx.AExprOp{
				Left: &lyx.ColumnRef{
					ColName:    "i",
					TableAlias: "",
				},
				Right: &lyx.AExprIConst{
					Value: 1,
				},
				Op: ">",
			},
			Right: &lyx.AExprOp{
				Left: &lyx.ColumnRef{
					ColName:    "i",
					TableAlias: "",
				},
				Right: &lyx.AExprIConst{
					Value: 5,
				},
				Op: "<",
			},
			Op: "and",
		}

	krs := []*kr.KeyRange{
		{ID: "kr1", LowerBound: []any{int64(-1)}, ColumnTypes: []string{"integer"}, ShardID: "sh1"},
		{ID: "kr2", LowerBound: []any{int64(0)}, ColumnTypes: []string{"integer"}, ShardID: "sh1"},
		{ID: "kr3", LowerBound: []any{int64(5)}, ColumnTypes: []string{"integer"}, ShardID: "sh2"},
		{ID: "kr4", LowerBound: []any{int64(10)}, ColumnTypes: []string{"integer"}, ShardID: "sh2"},
	}
	ds := &distributions.Distribution{ColTypes: []string{"integer"}}
	rel := &distributions.DistributedRelation{
		Name: "xx",
		DistributionKey: []distributions.DistributionKeyEntry{
			{Column: "i"},
		},
	}
	res, err := SolveShards(*whereStmt, krs, rel, ds)
	is.NoError(err)
	is.Equal([]string{"sh1"}, res)
}

func TestSolveShards(t *testing.T) {
	is := assert.New(t)
	whereStmt :=
		&lyx.AExprOp{
			Left: &lyx.AExprOp{
				Left: &lyx.ColumnRef{
					ColName:    "i",
					TableAlias: "",
				},
				Right: &lyx.AExprIConst{
					Value: 3,
				},
				Op: "<",
			},
			Right: &lyx.AExprOp{
				Left: &lyx.ColumnRef{
					ColName:    "i",
					TableAlias: "",
				},
				Right: &lyx.AExprIConst{
					Value: 5,
				},
				Op: ">",
			},
			Op: "or",
		}

	krs := []*kr.KeyRange{
		{ID: "kr1", LowerBound: []any{int64(-1)}, ColumnTypes: []string{"integer"}, ShardID: "sh1"},
		{ID: "kr2", LowerBound: []any{int64(0)}, ColumnTypes: []string{"integer"}, ShardID: "sh1"},
		{ID: "kr3", LowerBound: []any{int64(5)}, ColumnTypes: []string{"integer"}, ShardID: "sh2"},
		{ID: "kr4", LowerBound: []any{int64(10)}, ColumnTypes: []string{"integer"}, ShardID: "sh2"},
	}
	ds := &distributions.Distribution{ColTypes: []string{"integer"}}
	rel := &distributions.DistributedRelation{
		Name: "xx",
		DistributionKey: []distributions.DistributionKeyEntry{
			{Column: "i"},
		},
	}
	res, err := SolveShards(*whereStmt, krs, rel, ds)
	is.NoError(err)
	is.Equal([]string{"sh1", "sh2"}, res)
}
