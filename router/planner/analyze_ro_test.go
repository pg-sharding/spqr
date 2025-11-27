package planner_test

import (
	"testing"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/stretchr/testify/assert"
)

func TestCheckRoOnlyQuery(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   bool
	}

	for _, tt := range []tcase{
		{
			query: `select pg_is_in_recovery() from tt /* __spqr__target_session_attrs: smart-read-write *`,
			exp:   true,
		},
		{
			query: "with a as (select * from xx), b as (select * from yy) select * from a, b",
			exp:   true,
		},

		{
			query: "SELECT 1",
			exp:   true,
		},
		{
			query: "SELECT now()",
			exp:   true,
		},
		{
			query: "SELECT f()",
			exp:   false,
		},

		{
			query: "values((with d as (insert into zz select 1 returning *) table d))",
			exp:   false,
		},

		{
			query: "SELECT * from zz, f()",
			exp:   false,
		},

		{
			query: "DELETE FROM zz",
			exp:   false,
		},

		{
			query: "UPDATE zz SET  i = i + 1",
			exp:   false,
		},

		{
			query: "insert into zz values(1)",
			exp:   false,
		},

		{
			query: "with a as (select * from xx), b as (select * from yy), c as (insert into ff select * from a, b)  select * from c",
			exp:   false,
		},
	} {

		parserRes, _, err := lyx.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		act := planner.CheckRoOnlyQuery(parserRes[0])

		assert.Equal(tt.exp, act, tt.query)
	}
}
