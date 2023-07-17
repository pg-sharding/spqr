package clientinteractor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

func TestSimpleWhere(t *testing.T) {
	assert := assert.New(t)

	row := []string{"1", "2", "3"}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := spqrparser.WhereClauseLeaf{
		Op:     "=",
		ColRef: spqrparser.ColumnRef{ColName: "a"},
		Value:  "1",
	}
	expected := true

	actual, err := clientinteractor.MatchRow(row, rowDesc, where)
	assert.NoError(err)
	assert.Equal(expected, actual)
}

func TestSimpleNoMatchWhere(t *testing.T) {
	assert := assert.New(t)

	row := []string{"1", "2", "3"}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := spqrparser.WhereClauseLeaf{
		Op:     "=",
		ColRef: spqrparser.ColumnRef{ColName: "a"},
		Value:  "2",
	}
	expected := false

	actual, err := clientinteractor.MatchRow(row, rowDesc, where)
	assert.NoError(err)
	assert.Equal(expected, actual)
}

func TestAndNoMatchWhere(t *testing.T) {
	assert := assert.New(t)

	row := []string{"1", "2", "3"}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := spqrparser.WhereClauseOp{
		Op: "and",
		Left: spqrparser.WhereClauseLeaf{
			Op:     "=",
			ColRef: spqrparser.ColumnRef{ColName: "b"},
			Value:  "2",
		},
		Right: spqrparser.WhereClauseLeaf{
			Op:     "=",
			ColRef: spqrparser.ColumnRef{ColName: "a"},
			Value:  "2",
		},
	}
	expected := false

	actual, err := clientinteractor.MatchRow(row, rowDesc, where)
	assert.Nil(err)
	assert.Equal(expected, actual)
}

func TestOrMatchWhere(t *testing.T) {
	assert := assert.New(t)

	row := []string{"1", "2", "3"}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := spqrparser.WhereClauseOp{
		Op: "or",
		Left: spqrparser.WhereClauseLeaf{
			Op:     "=",
			ColRef: spqrparser.ColumnRef{ColName: "a"},
			Value:  "2",
		},
		Right: spqrparser.WhereClauseLeaf{
			Op:     "=",
			ColRef: spqrparser.ColumnRef{ColName: "b"},
			Value:  "2",
		},
	}
	expected := true

	actual, err := clientinteractor.MatchRow(row, rowDesc, where)
	assert.NoError(err)
	assert.Equal(expected, actual)
}
