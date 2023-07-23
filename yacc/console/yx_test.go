package spqrparser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

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
