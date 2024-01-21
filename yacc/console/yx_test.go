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
			query: "CREATE KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1;",
			exp: &spqrparser.Create{
				Element: &spqrparser.KeyRangeDefinition{
					ShardID:    "sh1",
					KeyRangeID: "krid1",
					Dataspace:  "default",
					LowerBound: &spqrparser.KeyRangeBound{
						Pivots: [][]byte{
							[]byte{
								0,
								0,
								0,
								0,
								0,
								0,
								0,
								0,
								1,
							},
						},
					},
					UpperBound: &spqrparser.KeyRangeBound{
						Pivots: [][]byte{
							[]byte{
								0,
								0,
								0,
								0,
								0,
								0,
								0,
								0,
								10,
							},
						},
					},
				},
			},

			err: nil,
		},

		{
			query: "CREATE KEY RANGE krid2 FROM 88888888-8888-8888-8888-888888888889 TO FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF ROUTE TO sh2;",
			exp: &spqrparser.Create{
				Element: &spqrparser.KeyRangeDefinition{
					ShardID:    "sh2",
					KeyRangeID: "krid2",
					Dataspace:  "default",
					LowerBound: &spqrparser.KeyRangeBound{
						Pivots: [][]byte{
							[]byte("88888888-8888-8888-8888-888888888889"),
						},
					},
					UpperBound: &spqrparser.KeyRangeBound{
						Pivots: [][]byte{
							[]byte("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"),
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

func TestSplitKeyRange(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "SPLIT KEY RANGE krid3 FROM krid1 BY 5;",
			exp: &spqrparser.SplitKeyRange{
				Border:         []byte("5"),
				KeyRangeFromID: "krid1",
				KeyRangeID:     "krid3",
			},
			err: nil,
		},
	} {

		tmp, err := spqrparser.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestAttachTable(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "ALTER DATASPACE ds1 ATTACH TABLE t (id);",
			exp: &spqrparser.AttachTable{
				Relation: &spqrparser.ShardedRelaion{
					Name:    "t",
					Columns: []string{"id"},
				},
				Dataspace: &spqrparser.DataspaceSelector{ID: "ds1"},
			},
			err: nil,
		},
	} {

		tmp, err := spqrparser.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestDataspace(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "CREATE DATASPACE db1 SHARDING COLUMN TYPES integer;",
			exp: &spqrparser.Create{
				Element: &spqrparser.DataspaceDefinition{
					ID: "db1",
					ColTypes: []string{
						"integer",
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE DATASPACE db1 SHARDING COLUMN TYPES varchar, varchar;",
			exp: &spqrparser.Create{
				Element: &spqrparser.DataspaceDefinition{
					ID: "db1",
					ColTypes: []string{
						"varchar",
						"varchar",
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE DATASPACE db1 SHARDING COLUMN TYPES varchar, varchar RELATIONS t(id, id2), t2(indx, indx2)",
			exp: &spqrparser.Create{
				Element: &spqrparser.DataspaceDefinition{
					ID: "db1",
					ColTypes: []string{
						"varchar",
						"varchar",
					},
					Relations: []*spqrparser.ShardedRelaion{
						{
							Name:    "t",
							Columns: []string{"id", "id2"},
						},
						{
							Name:    "t2",
							Columns: []string{"indx", "indx2"},
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
