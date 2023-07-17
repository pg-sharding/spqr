package spqrparser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

func TestSimpleWhere(t *testing.T) {
	assert := assert.New(t)

	stmt, err := spqrparser.Parse("SHOW clients where user = usr1;")
	assert.NoError(err)
	show, ok := stmt.(*spqrparser.Show)
	assert.True(ok)
	whereClause, ok := show.Where.(spqrparser.WhereClauseLeaf)
	assert.True(ok)
	assert.Equal(spqrparser.WhereClauseLeaf{
		Op:     "=",
		ColRef: spqrparser.ColumnRef{ColName: "user"},
		Value:  "usr1",
	}, whereClause)
}

func TestNestedeWhere(t *testing.T) {
	assert := assert.New(t)

	stmt, err := spqrparser.Parse("SHOW clients where user = usr1 or dbname = db1 and 1 = 1;")
	assert.NoError(err)
	show, ok := stmt.(*spqrparser.Show)
	assert.True(ok)
	whereClause, ok := show.Where.(spqrparser.WhereClauseOp)
	assert.True(ok)

	expected := spqrparser.WhereClauseOp{
		Op: "or",
		Left: spqrparser.WhereClauseLeaf{
			Op:     "=",
			ColRef: spqrparser.ColumnRef{ColName: "user"},
			Value:  "usr1",
		},
		Right: spqrparser.WhereClauseOp{
			Op: "and",
			Left: spqrparser.WhereClauseLeaf{
				Op:     "=",
				ColRef: spqrparser.ColumnRef{ColName: "dbname"},
				Value:  "db1",
			},
			Right: spqrparser.WhereClauseLeaf{
				Op:     "=",
				ColRef: spqrparser.ColumnRef{ColName: "1"},
				Value:  "1",
			},
		},
	}
	assert.Equal(expected, whereClause)
}

func TestNoWhere(t *testing.T) {
	assert := assert.New(t)

	stmt, err := spqrparser.Parse("SHOW clients;")
	assert.NoError(err)
	show, ok := stmt.(*spqrparser.Show)
	assert.True(ok)
	_, ok = show.Where.(spqrparser.WhereClauseEmpty)
	assert.True(ok)
}
