package spqrparser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

func TestSimpleTrace(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	/*  */
	for _, tt := range []tcase{
		{
			query: "START TRACE ALL MESSAGES",
			exp: &spqrparser.TraceStmt{
				All: true,
			},
			err: nil,
		},

		{
			query: "START TRACE CLIENT 129191;",
			exp: &spqrparser.TraceStmt{
				Client: 129191,
			},
			err: nil,
		},

		{
			query: "STOP TRACE MESSAGES",
			exp:   &spqrparser.StopTraceStmt{},
			err:   nil,
		},
	} {
		tmp, err := spqrparser.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestSimpleShow(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	/* POOLS STATS LISTS SERVERS CLIENTS DATABASES BACKEND_CONNECTIONS */
	for _, tt := range []tcase{
		{
			query: "SHOW version",
			exp: &spqrparser.Show{
				Cmd:   spqrparser.VersionStr,
				Where: spqrparser.WhereClauseEmpty{},
			},
			err: nil,
		},
		/* case insensetive */
		{
			query: "ShOw versIon",
			exp: &spqrparser.Show{
				Cmd:   spqrparser.VersionStr,
				Where: spqrparser.WhereClauseEmpty{},
			},
			err: nil,
		},

		{
			query: "ShOw pools",
			exp: &spqrparser.Show{
				Cmd:   spqrparser.PoolsStr,
				Where: spqrparser.WhereClauseEmpty{},
			},
			err: nil,
		},
		{
			query: "ShOw clients",
			exp: &spqrparser.Show{
				Cmd:   spqrparser.ClientsStr,
				Where: spqrparser.WhereClauseEmpty{},
			},
			err: nil,
		},
		{
			query: "ShOw DATABASES",
			exp: &spqrparser.Show{
				Cmd:   spqrparser.DatabasesStr,
				Where: spqrparser.WhereClauseEmpty{},
			},
			err: nil,
		},
		{
			query: "ShOw BACKEND_CONNECTIONS",
			exp: &spqrparser.Show{
				Cmd:   spqrparser.BackendConnectionsStr,
				Where: spqrparser.WhereClauseEmpty{},
			},
			err: nil,
		},
		{
			query: "kill client 824636929312;",
			exp: &spqrparser.Kill{
				Cmd:    spqrparser.ClientStr,
				Target: 824636929312,
			},
			err: nil,
		},
	} {
		tmp, err := spqrparser.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestSimpleWhere(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "SHOW clients where user = 'usr1';",
			exp: &spqrparser.Show{
				Cmd: spqrparser.ClientsStr,
				Where: spqrparser.WhereClauseLeaf{
					Op:     "=",
					ColRef: spqrparser.ColumnRef{ColName: "user"},
					Value:  "usr1",
				},
			},
			err: nil,
		},
	} {

		tmp, err := spqrparser.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestNestedWhere(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "SHOW clients where user = 'usr1' or dbname = 'db1';",
			exp: &spqrparser.Show{
				Cmd: spqrparser.ClientsStr,
				Where: spqrparser.WhereClauseOp{
					Op: "OR",
					Left: spqrparser.WhereClauseLeaf{
						Op:     "=",
						ColRef: spqrparser.ColumnRef{ColName: "user"},
						Value:  "usr1",
					},
					Right: spqrparser.WhereClauseLeaf{
						Op:     "=",
						ColRef: spqrparser.ColumnRef{ColName: "dbname"},
						Value:  "db1",
					},
				},
			},
			err: nil,
		},
	} {

		tmp, err := spqrparser.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestKeyRange(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1;",
			exp: &spqrparser.Create{
				Element: &spqrparser.KeyRangeDefinition{
					ShardID:    "sh1",
					KeyRangeID: "krid1",
					Dataspace:  "default",
					LowerBound: []byte("1"),
					UpperBound: []byte("10"),
				},
			},
			err: nil,
		},

		{
			query: "ADD KEY RANGE krid2 FROM 88888888-8888-8888-8888-888888888889 TO FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF ROUTE TO sh2;",
			exp: &spqrparser.Create{
				Element: &spqrparser.KeyRangeDefinition{
					ShardID:    "sh2",
					KeyRangeID: "krid2",
					Dataspace:  "default",
					LowerBound: []byte("88888888-8888-8888-8888-888888888889"),
					UpperBound: []byte("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"),
				},
			},
			err: nil,
		},
	} {

		tmp, err := spqrparser.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestShardingRule(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "ADD SHARDING RULE rule1 COLUMNS id;",
			exp: &spqrparser.Create{
				Element: &spqrparser.ShardingRuleDefinition{
					ID:        "rule1",
					TableName: "",
					Dataspace: "default",
					Entries: []spqrparser.ShardingRuleEntry{
						{
							Column: "id",
						},
					},
				},
			},
			err: nil,
		},
	} {

		tmp, err := spqrparser.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}
