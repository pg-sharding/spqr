package spqrparser_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/router/rfqn"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

func TestSimple(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	/*  */
	for _, tt := range []tcase{
		{
			query: "\nSHOW relations",
			exp: &spqrparser.Show{
				Cmd:     spqrparser.RelationsStr,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
			err: nil,
		},

		{
			query: "\nSHOW \n relations",
			exp: &spqrparser.Show{
				Cmd:     spqrparser.RelationsStr,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
			err: nil,
		},

		{
			query: "\nSHOW \n relations \n\n;",
			exp: &spqrparser.Show{
				Cmd:     spqrparser.RelationsStr,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
			err: nil,
		},
	} {
		tmp, err := spqrparser.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

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
				Cmd:     spqrparser.VersionStr,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
			err: nil,
		},
		/* case insensitive */
		{
			query: "ShOw versIon",
			exp: &spqrparser.Show{
				Cmd:     spqrparser.VersionStr,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
			err: nil,
		},

		{
			query: "ShOw pools",
			exp: &spqrparser.Show{
				Cmd:     spqrparser.PoolsStr,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
			err: nil,
		},

		{
			query: "ShOw instance",
			exp: &spqrparser.Show{
				Cmd:     spqrparser.InstanceStr,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
			err: nil,
		},
		{
			query: "ShOw clients",
			exp: &spqrparser.Show{
				Cmd:     spqrparser.ClientsStr,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
			err: nil,
		},
		{
			query: "ShOw DATABASES",
			exp: &spqrparser.Show{
				Cmd:     spqrparser.DatabasesStr,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
			err: nil,
		},
		{
			query: "ShOw BACKEND_CONNECTIONS",
			exp: &spqrparser.Show{
				Cmd:     spqrparser.BackendConnectionsStr,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
			err: nil,
		},
		{
			query: "SHOW move_stats",
			exp: &spqrparser.Show{
				Cmd:     spqrparser.MoveStatsStr,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
		},
		{
			query: "SHOW users",
			exp: &spqrparser.Show{
				Cmd:     spqrparser.Users,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
		},
		{
			query: "SHOW tsa_cache",
			exp: &spqrparser.Show{
				Cmd:     spqrparser.TsaCacheStr,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
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
				Where: &lyx.AExprOp{
					Left: &lyx.ColumnRef{
						ColName: "user",
					},
					Right: &lyx.AExprSConst{
						Value: "usr1",
					},
					Op: "=",
				},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
			err: nil,
		},
		{
			query: `SHOW relations WHERE distribution_id = 'ds1';`,
			exp: &spqrparser.Show{
				Cmd: spqrparser.RelationsStr,
				Where: &lyx.AExprOp{
					Left: &lyx.ColumnRef{
						ColName: "distribution_id",
					},
					Right: &lyx.AExprSConst{
						Value: "ds1",
					},
					Op: "=",
				},
				GroupBy: spqrparser.GroupByClauseEmpty{},
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
				Where: &lyx.AExprOp{
					Op: "or",
					Left: &lyx.AExprOp{
						Op: "=",
						Left: &lyx.ColumnRef{
							ColName: "user",
						},
						Right: &lyx.AExprSConst{
							Value: "usr1",
						},
					},
					Right: &lyx.AExprOp{

						Op: "=",
						Left: &lyx.ColumnRef{
							ColName: "dbname",
						},
						Right: &lyx.AExprSConst{
							Value: "db1",
						},
					},
				},
				GroupBy: spqrparser.GroupByClauseEmpty{},
			},
			err: nil,
		},
	} {

		tmp, err := spqrparser.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestGroupBy(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query  string
		errmsg string
		exp    spqrparser.Statement
		err    error
	}

	for _, tt := range []tcase{
		{
			query: "SHOW backend_connections GROUP BY hostname;",
			exp: &spqrparser.Show{
				Cmd: spqrparser.BackendConnectionsStr,

				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupBy{Col: []spqrparser.ColumnRef{{ColName: "hostname"}}},
			},
			err: nil,
		},
		{
			query: "SHOW backend_connections GROUP BY user, dbname",
			exp: &spqrparser.Show{
				Cmd:     spqrparser.BackendConnectionsStr,
				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupBy{Col: []spqrparser.ColumnRef{{ColName: "user"}, {ColName: "dbname"}}},
			},
			err: nil,
		},
		{
			query:  "SHOW backend_connections GROUP BY ",
			exp:    nil,
			errmsg: "syntax error",
		},
	} {

		tmp, err := spqrparser.Parse(tt.query)

		if tt.errmsg != "" {
			assert.ErrorContains(err, tt.errmsg, tt.query)
		} else {
			assert.NoError(err, "query %s", tt.query)

			assert.Equal(tt.exp, tmp, "query %s", tt.query)
		}
	}
}

func TestOrderBy(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "SHOW backend_connections ORDER BY hostname;",
			exp: &spqrparser.Show{
				Cmd: spqrparser.BackendConnectionsStr,

				Where:   &lyx.AExprEmpty{},
				GroupBy: spqrparser.GroupByClauseEmpty{},
				Order: &spqrparser.Order{
					Col:        spqrparser.ColumnRef{ColName: "hostname"},
					OptAscDesc: &spqrparser.SortByDefault{},
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

func TestRedistribute(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "REDISTRIBUTE KEY RANGE kr1 TO sh2",
			exp: &spqrparser.RedistributeKeyRange{
				KeyRangeID:  "kr1",
				DestShardID: "sh2",
				BatchSize:   -1,
				Check:       true,
				Apply:       true,
			},
			err: nil,
		},

		{
			query: "REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 500",
			exp: &spqrparser.RedistributeKeyRange{
				KeyRangeID:  "kr1",
				DestShardID: "sh2",
				BatchSize:   500,
				Check:       true,
				Apply:       true,
			},
			err: nil,
		},
		{
			query: "REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE 500 NOWAIT",
			exp: &spqrparser.RedistributeKeyRange{
				KeyRangeID:  "kr1",
				DestShardID: "sh2",
				BatchSize:   500,
				Check:       true,
				NoWait:      true,
			},
			err: nil,
		},
		{
			query: "REDISTRIBUTE KEY RANGE kr1 TO sh2 BATCH SIZE -1",
			exp:   nil,
			err:   fmt.Errorf("syntax error"),
		},
		{
			query: "REDISTRIBUTE KEY RANGE kr1 TO sh2",
			exp: &spqrparser.RedistributeKeyRange{
				KeyRangeID:  "kr1",
				DestShardID: "sh2",
				BatchSize:   -1,
				Check:       true,
				Apply:       true,
			},
			err: nil,
		},
		{
			query: "REDISTRIBUTE KEY RANGE kr1 TO sh2 CHECK",
			exp: &spqrparser.RedistributeKeyRange{
				KeyRangeID:  "kr1",
				DestShardID: "sh2",
				BatchSize:   -1,
				Check:       true,
				Apply:       false,
			},
			err: nil,
		},
		{
			query: "REDISTRIBUTE KEY RANGE kr1 TO sh2 APPLY",
			exp: &spqrparser.RedistributeKeyRange{
				KeyRangeID:  "kr1",
				DestShardID: "sh2",
				BatchSize:   -1,
				Check:       true,
				Apply:       true,
			},
			err: nil,
		},
	} {

		tmp, err := spqrparser.Parse(tt.query)

		if err != nil {
			assert.EqualError(err, tt.err.Error())
		} else {
			assert.NoError(err, "query %s", tt.query)
		}

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
			query: "CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
			exp: &spqrparser.Create{
				Element: &spqrparser.KeyRangeDefinition{
					ShardID:    "sh1",
					KeyRangeID: "krid1",
					Distribution: &spqrparser.DistributionSelector{
						ID: "ds1",
					},
					LowerBound: &spqrparser.KeyRangeBound{
						Pivots: [][]byte{
							{2, 0, 0, 0, 0, 0, 0, 0, 0, 0},
						},
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE KEY RANGE krid1 FROM 1 ROUTE TO 'sh-1' FOR DISTRIBUTION ds1;",
			exp: &spqrparser.Create{
				Element: &spqrparser.KeyRangeDefinition{
					ShardID:    "sh-1",
					KeyRangeID: "krid1",
					Distribution: &spqrparser.DistributionSelector{
						ID: "ds1",
					},
					LowerBound: &spqrparser.KeyRangeBound{
						Pivots: [][]byte{
							{2, 0, 0, 0, 0, 0, 0, 0, 0, 0},
						},
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE KEY RANGE krid1 FROM -10 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
			exp: &spqrparser.Create{
				Element: &spqrparser.KeyRangeDefinition{
					ShardID:    "sh1",
					KeyRangeID: "krid1",
					Distribution: &spqrparser.DistributionSelector{
						ID: "ds1",
					},
					LowerBound: &spqrparser.KeyRangeBound{
						Pivots: [][]byte{
							{0x13, 0, 0, 0, 0, 0, 0, 0, 0, 0},
						},
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE KEY RANGE krid1 FROM -20 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
			exp: &spqrparser.Create{
				Element: &spqrparser.KeyRangeDefinition{
					ShardID:    "sh1",
					KeyRangeID: "krid1",
					Distribution: &spqrparser.DistributionSelector{
						ID: "ds1",
					},
					LowerBound: &spqrparser.KeyRangeBound{
						Pivots: [][]byte{
							{0x27, 0, 0, 0, 0, 0, 0, 0, 0, 0},
						},
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE KEY RANGE krid2 FROM 4611686018427387904 ROUTE TO sh2 FOR DISTRIBUTION ds1;",
			exp: &spqrparser.Create{
				Element: &spqrparser.KeyRangeDefinition{
					ShardID:    "sh2",
					KeyRangeID: "krid2",
					Distribution: &spqrparser.DistributionSelector{
						ID: "ds1",
					},
					LowerBound: &spqrparser.KeyRangeBound{
						Pivots: [][]byte{
							{128, 128, 128, 128, 128, 128, 128, 128, 128, 1},
						},
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE KEY RANGE krid2 FROM '88888888-8888-8888-8888-888888888889' ROUTE TO sh2 FOR DISTRIBUTION ds1;",
			exp: &spqrparser.Create{
				Element: &spqrparser.KeyRangeDefinition{
					ShardID:    "sh2",
					KeyRangeID: "krid2",
					Distribution: &spqrparser.DistributionSelector{
						ID: "ds1",
					},
					LowerBound: &spqrparser.KeyRangeBound{
						Pivots: [][]byte{
							[]byte("88888888-8888-8888-8888-888888888889"),
						},
					},
				},
			},
			err: nil,
		},

		{
			query: `
			CREATE KEY RANGE krid1 FROM 0, 'a' ROUTE TO sh1 FOR DISTRIBUTION ds1;`,

			exp: &spqrparser.Create{
				Element: &spqrparser.KeyRangeDefinition{
					ShardID:    "sh1",
					KeyRangeID: "krid1",
					Distribution: &spqrparser.DistributionSelector{
						ID: "ds1",
					},
					LowerBound: &spqrparser.KeyRangeBound{
						Pivots: [][]byte{
							{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
							[]byte("a"),
						},
					},
				},
			},
			err: nil,
		},

		{
			query: "CREATE KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
			exp:   nil,
			err:   fmt.Errorf("syntax error"),
		},
	} {

		tmp, err := spqrparser.Parse(tt.query)

		if tt.err == nil {
			assert.NoError(err, "query %s", tt.query)
		} else {
			assert.Error(err, "query %s", tt.query)
		}

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestRegisterRouter(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: `REGISTER ROUTER r1 ADDRESS "someRandomHost:1234"`,
			exp: &spqrparser.RegisterRouter{
				ID:   "r1",
				Addr: "someRandomHost:1234",
			},
		},
		{
			query: `REGISTER ROUTER 'r-1' ADDRESS "someRandomHost:1234"`,
			exp: &spqrparser.RegisterRouter{
				ID:   "r-1",
				Addr: "someRandomHost:1234",
			},
		},
		{
			query: `REGISTER ROUTER r1 ADDRESS 'az-host:1234'`,
			exp: &spqrparser.RegisterRouter{
				ID:   "r1",
				Addr: "az-host:1234",
			},
		},
	} {

		tmp, err := spqrparser.Parse(tt.query)

		if tt.err == nil {
			assert.NoError(err, "query %s", tt.query)
		} else {
			assert.Error(err, "query %s", tt.query)
		}

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}

}

func TestKeyRangeBordersSuccess(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "CREATE KEY RANGE krid1 FROM 9223372036854775807 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
			exp: &spqrparser.Create{
				Element: &spqrparser.KeyRangeDefinition{
					ShardID:    "sh1",
					KeyRangeID: "krid1",
					Distribution: &spqrparser.DistributionSelector{
						ID: "ds1",
					},
					LowerBound: &spqrparser.KeyRangeBound{
						Pivots: [][]byte{
							{254, 255, 255, 255, 255, 255, 255, 255, 255, 1},
						},
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE KEY RANGE krid1 FROM -9223372036854775808 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
			exp: &spqrparser.Create{
				Element: &spqrparser.KeyRangeDefinition{
					ShardID:    "sh1",
					KeyRangeID: "krid1",
					Distribution: &spqrparser.DistributionSelector{
						ID: "ds1",
					},
					LowerBound: &spqrparser.KeyRangeBound{
						Pivots: [][]byte{
							{255, 255, 255, 255, 255, 255, 255, 255, 255, 1},
						},
					},
				},
			},
			err: nil,
		},
	} {

		tmp, err := spqrparser.Parse(tt.query)

		if tt.err == nil {
			assert.NoError(err, "query %s", tt.query)
		} else {
			assert.Error(err, "query %s", tt.query)
		}

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestKeyRangeBordersFail(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "CREATE KEY RANGE krid1 FROM 9223372036854775809 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
			exp:   nil,
			err:   fmt.Errorf(spqrparser.SIGNED_INT_RANGE_ERROR),
		},
		{
			query: "CREATE KEY RANGE krid1 FROM -9223372036854775809 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
			exp:   nil,
			err:   fmt.Errorf(spqrparser.SIGNED_INT_RANGE_ERROR),
		},
		{
			query: "CREATE KEY RANGE krid1 FROM 92233720368547758099999999 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
			exp:   nil,
			err:   fmt.Errorf("syntax error"),
		},
		{
			query: "CREATE KEY RANGE krid1 FROM -9223372036854775809999999 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
			exp:   nil,
			err:   fmt.Errorf("syntax error"),
		},
	} {
		tmp, err := spqrparser.Parse(tt.query)

		if tt.err == nil {
			assert.NoError(err, "query %s", tt.query)
		} else {
			assert.Error(err, "query %s", tt.query)
		}

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
				Border: &spqrparser.KeyRangeBound{
					Pivots: [][]byte{
						{10, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					},
				},
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

	tmp, err := spqrparser.Parse("ATTACH TABLE t TO DISTRIBUTION ds1;")
	assert.Error(err)
	assert.Equal(nil, tmp, "query %s", "ATTACH TABLE t TO DISTRIBUTION ds1;")
}

func TestAlter(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION t DISTRIBUTION KEY id;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "t",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE DISTRIBUTED RELATION t DISTRIBUTION KEY id IN ds1;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "t",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		{
			query: "CREATE RELATION t (id) IN ds1;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "t",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		{
			query: "CREATE RELATION t (id);",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "default"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "t",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		{
			query: "CREATE DISTRIBUTED RELATION t (id) IN ds1;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "t",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		{
			query: "CREATE DISTRIBUTED RELATION t (MURMUR [id1 INT HASH, id2 VARCHAR HASH]);",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "default"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "t",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										HashFunction: "murmur",
										Expr: []spqrparser.TypedColRef{
											{
												Column: "id1",
												Type:   "uinteger",
											},
											{
												Column: "id2",
												Type:   "varchar hashed",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		{
			query: "CREATE DISTRIBUTED RELATION 'ss' (uid HASH MURMUR) IN dd;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "dd"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "ss",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column:       "uid",
										HashFunction: "murmur",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		{
			query: `CREATE DISTRIBUTED RELATION "ss" (uid HASH MURMUR) IN dd;`,
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "dd"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "ss",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column:       "uid",
										HashFunction: "murmur",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		{
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION t DISTRIBUTION KEY id1, id2;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "t",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id1",
									},
									{
										Column: "id2",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		{
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION t (id1, id2);",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "t",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id1",
									},
									{
										Column: "id2",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},
		{
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION t DISTRIBUTION KEY id1, id2 HASH FUNCTION murmur;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "t",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id1",
									},
									{
										Column:       "id2",
										HashFunction: "murmur",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		{
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION t (id1, id2 HASH FUNCTION murmur);",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "t",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id1",
									},
									{
										Column:       "id2",
										HashFunction: "murmur",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		{
			query: `
		ALTER DISTRIBUTION 
			ds1 
		ATTACH
			RELATION t DISTRIBUTION KEY id1, id2 HASH FUNCTION murmur
			RELATION t2 DISTRIBUTION KEY xd1, xd2 HASH FUNCTION city
			`,
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "t",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id1",
									},
									{
										Column:       "id2",
										HashFunction: "murmur",
									},
								},
							},
							{
								Name: "t2",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "xd1",
									},
									{
										Column:       "xd2",
										HashFunction: "city",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		{
			query: "ALTER DISTRIBUTION ds1 DETACH RELATION t;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.DetachRelation{
						RelationName: &rfqn.RelationFQN{RelationName: "t"},
					},
				},
			},
			err: nil,
		},
		{
			query: "ALTER DISTRIBUTION ds1 DETACH RELATION schema1.t;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.DetachRelation{
						RelationName: &rfqn.RelationFQN{RelationName: "t", SchemaName: "schema1"},
					},
				},
			},
			err: nil,
		},
		{
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION t DISTRIBUTION KEY id1 AUTO INCREMENT id1, id2;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "t",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id1",
									},
								},
								AutoIncrementEntries: []*spqrparser.AutoIncrementEntry{
									{
										Column: "id1",
									},
									{
										Column: "id2",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION t DISTRIBUTION KEY id1 AUTO INCREMENT id1 START 123, id2 START 321;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name: "t",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id1",
									},
								},
								AutoIncrementEntries: []*spqrparser.AutoIncrementEntry{
									{
										Column: "id1",
										Start:  123,
									},
									{
										Column: "id2",
										Start:  321,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION t DISTRIBUTION KEY id SCHEMA test;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name:       "t",
								SchemaName: "test",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},
		{
			query: "ALTER DISTRIBUTION ds1 ALTER RELATION t DISTRIBUTION KEY id;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AlterRelationV2{
						RelationName: "t",
						Element: &spqrparser.AlterRelationDistributionKey{
							DistributionKey: []spqrparser.DistributionKeyEntry{
								{
									Column: "id",
								},
							},
						},
					},
				},
			},
			err: nil,
		},
		{
			query: "ALTER DISTRIBUTION ds1 ALTER RELATION t SCHEMA test;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AlterRelationV2{
						RelationName: "t",
						Element: &spqrparser.AlterRelationSchema{
							SchemaName: "test",
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

func TestDistribution(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "CREATE DISTRIBUTION db1 COLUMN TYPES integer;",
			exp: &spqrparser.Create{
				Element: &spqrparser.DistributionDefinition{
					ID: "db1",
					ColTypes: []string{
						"integer",
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE DISTRIBUTION db1 COLUMN TYPES varchar hash;",
			exp: &spqrparser.Create{
				Element: &spqrparser.DistributionDefinition{
					ID: "db1",
					ColTypes: []string{
						"varchar hashed",
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE DISTRIBUTION db1 COLUMN TYPES varchar, varchar;",
			exp: &spqrparser.Create{
				Element: &spqrparser.DistributionDefinition{
					ID: "db1",
					ColTypes: []string{
						"varchar",
						"varchar",
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

func TestReferenceRelation(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{

		{
			query: "CREATE REFERENCE TABLE xtab",
			exp: &spqrparser.Create{
				Element: &spqrparser.ReferenceRelationDefinition{
					TableName: &rfqn.RelationFQN{
						RelationName: "xtab",
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE REFERENCE TABLE xtab",
			exp: &spqrparser.Create{
				Element: &spqrparser.ReferenceRelationDefinition{
					TableName: &rfqn.RelationFQN{
						RelationName: "xtab",
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE REFERENCE RELATION xtab ON sh1, sh2, sh3",
			exp: &spqrparser.Create{
				Element: &spqrparser.ReferenceRelationDefinition{
					TableName: &rfqn.RelationFQN{
						RelationName: "xtab",
					},
					ShardIds: []string{"sh1", "sh2", "sh3"},
				},
			},
			err: nil,
		},
		{
			query: "CREATE REFERENCE RELATION xtab ON SHARDS sh1, sh2, sh3",
			exp: &spqrparser.Create{
				Element: &spqrparser.ReferenceRelationDefinition{
					TableName: &rfqn.RelationFQN{
						RelationName: "xtab",
					},
					ShardIds: []string{"sh1", "sh2", "sh3"},
				},
			},
			err: nil,
		},
		{
			query: "CREATE REFERENCE TABLE xtab AUTO INCREMENT id",
			exp: &spqrparser.Create{
				Element: &spqrparser.ReferenceRelationDefinition{
					TableName: &rfqn.RelationFQN{
						RelationName: "xtab",
					},
					AutoIncrementEntries: []*spqrparser.AutoIncrementEntry{
						{
							Column: "id",
						},
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE REFERENCE TABLE xtab AUTO INCREMENT id START 42",
			exp: &spqrparser.Create{
				Element: &spqrparser.ReferenceRelationDefinition{
					TableName: &rfqn.RelationFQN{
						RelationName: "xtab",
					},
					AutoIncrementEntries: []*spqrparser.AutoIncrementEntry{
						{
							Column: "id",
							Start:  42,
						},
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE REFERENCE TABLE xtab AUTO INCREMENT id1 START 42, id2 START 43",
			exp: &spqrparser.Create{
				Element: &spqrparser.ReferenceRelationDefinition{
					TableName: &rfqn.RelationFQN{
						RelationName: "xtab",
					},
					AutoIncrementEntries: []*spqrparser.AutoIncrementEntry{
						{
							Column: "id1",
							Start:  42,
						},
						{
							Column: "id2",
							Start:  43,
						},
					},
				},
			},
			err: nil,
		},
		{
			query: `DROP REFERENCE RELATION r1`,
			exp: &spqrparser.Drop{
				Element: &spqrparser.ReferenceRelationSelector{
					ID: "r1",
				},
			},
		},
		{
			query: `DROP REFERENCE TABLE r1`,
			exp: &spqrparser.Drop{
				Element: &spqrparser.ReferenceRelationSelector{
					ID: "r1",
				},
			},
		},
		{
			query: "CREATE DISTRIBUTION db1 COLUMN TYPES varchar hash;",
			exp: &spqrparser.Create{
				Element: &spqrparser.DistributionDefinition{
					ID: "db1",
					ColTypes: []string{
						"varchar hashed",
					},
				},
			},
			err: nil,
		},
		{
			query: "CREATE DISTRIBUTION db1 COLUMN TYPES varchar, varchar;",
			exp: &spqrparser.Create{
				Element: &spqrparser.DistributionDefinition{
					ID: "db1",
					ColTypes: []string{
						"varchar",
						"varchar",
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

func TestShard(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: `CREATE SHARD sh1 WITH HOSTS "localhost:6432";`,
			exp: &spqrparser.Create{
				Element: &spqrparser.ShardDefinition{
					Id:    "sh1",
					Hosts: []string{"localhost:6432"},
				},
			},
			err: nil,
		},
		{
			query: `CREATE SHARD sh1 WITH HOSTS "localhost:6432", "other_hosts:6432";`,
			exp: &spqrparser.Create{
				Element: &spqrparser.ShardDefinition{
					Id: "sh1",
					Hosts: []string{
						"localhost:6432",
						"other_hosts:6432",
					},
				},
			},
			err: nil,
		},
		{
			query: "DROP SHARD sh1;",
			exp: &spqrparser.Drop{
				Element: &spqrparser.ShardSelector{
					ID: "sh1",
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

func TestRefresh(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	/*  */
	for _, tt := range []tcase{
		{
			query: "INVALIDATE CACHE",
			exp: &spqrparser.Invalidate{
				Target: spqrparser.SchemaCacheInvalidateTarget,
			},
			err: nil,
		},
		{
			query: "INVALIDATE SCHEMA CACHE",
			exp: &spqrparser.Invalidate{
				Target: spqrparser.SchemaCacheInvalidateTarget,
			},
			err: nil,
		},
		{
			query: "INVALIDATE BACKENDS",
			exp: &spqrparser.Invalidate{
				Target: spqrparser.BackendConnectionsInvalidateTarget,
			},
			err: nil,
		},
		{
			query: "INVALIDATE STALE CLIENTs",
			exp: &spqrparser.Invalidate{
				Target: spqrparser.StaleClientsInvalidateTarget,
			},
			err: nil,
		},
	} {
		tmp, err := spqrparser.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestSyncReferenceTable(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "SYNC REFERENCE TABLES ON sh1",
			exp: &spqrparser.SyncReferenceTables{
				ShardID:          "sh1",
				RelationSelector: "*",
			},
			err: nil,
		},
		{
			query: "SYNC REFERENCE TABLE ref_tt ON sh1",
			exp: &spqrparser.SyncReferenceTables{
				ShardID:          "sh1",
				RelationSelector: "ref_tt",
			},
			err: nil,
		},
	} {
		tmp, err := spqrparser.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestRetryMoveTaskGroup(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "RETRY MOVE TASK GROUP",
			exp:   nil,
			err:   fmt.Errorf("syntax error"),
		},
		{
			query: "RETRY TASK GROUP",
			exp:   nil,
			err:   fmt.Errorf("syntax error"),
		},
		{
			query: "RETRY MOVE TASK GROUP tg_id",
			exp:   &spqrparser.RetryMoveTaskGroup{ID: "tg_id"},
			err:   nil,
		},
		{
			query: "RETRY TASK GROUP tg_id",
			exp:   &spqrparser.RetryMoveTaskGroup{ID: "tg_id"},
			err:   nil,
		},
	} {
		tmp, err := spqrparser.Parse(tt.query)

		if tt.err != nil {
			assert.Error(err, "query %s", tt.query)
		} else {
			assert.NoError(err, "query %s", tt.query)
		}

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestStopMoveTaskGroup(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "STOP MOVE TASK GROUP",
			exp:   nil,
			err:   fmt.Errorf("syntax error"),
		},
		{
			query: "STOP TASK GROUP",
			exp:   nil,
			err:   fmt.Errorf("syntax error"),
		},
		{
			query: "STOP MOVE TASK GROUP tg_id",
			exp:   &spqrparser.StopMoveTaskGroup{ID: "tg_id"},
			err:   nil,
		},
		{
			query: "STOP TASK GROUP tg_id",
			exp:   &spqrparser.StopMoveTaskGroup{ID: "tg_id"},
			err:   nil,
		},
	} {
		tmp, err := spqrparser.Parse(tt.query)

		if tt.err != nil {
			assert.Error(err, "query %s", tt.query)
		} else {
			assert.NoError(err, "query %s", tt.query)
		}

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestDistributionDefaultShard(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "CREATE DISTRIBUTION ds1 DEFAULT SHARD shard1;",
			exp: &spqrparser.Create{
				Element: &spqrparser.DistributionDefinition{
					ID:           "ds1",
					DefaultShard: "shard1",
				},
			},
			err: nil,
		},
		{
			query: "CREATE DISTRIBUTION ds1 COLUMN TYPES integer DEFAULT SHARD shard1;",
			exp: &spqrparser.Create{
				Element: &spqrparser.DistributionDefinition{
					ID: "ds1",
					ColTypes: []string{
						"integer",
					},
					DefaultShard: "shard1",
				},
			},
			err: nil,
		},
		{
			query: "ALTER DISTRIBUTION distr1 ADD DEFAULT SHARD sh1;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "distr1"},
					Element: &spqrparser.AlterDefaultShard{
						Shard: "sh1",
					},
				},
			},
			err: nil,
		},
		{
			query: "ALTER DISTRIBUTION distr1 DROP DEFAULT SHARD;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "distr1"},
					Element:      &spqrparser.DropDefaultShard{},
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

func TestRelationQualifiedName(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION sch1.table1 DISTRIBUTION KEY id;",
			exp: &spqrparser.Alter{
				Element: &spqrparser.AlterDistribution{
					Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
					Element: &spqrparser.AttachRelation{
						Relations: []*spqrparser.DistributedRelation{
							{
								Name:       "table1",
								SchemaName: "sch1",
								DistributionKey: []spqrparser.DistributionKeyEntry{
									{
										Column: "id",
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},
		{
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION sch1.table1 DISTRIBUTION KEY id SCHEMA sch1;",
			exp:   nil,
			err:   fmt.Errorf("it is forbidden to use both a qualified relation name and the keyword SCHEMA"),
		},
	} {
		tmp, err := spqrparser.Parse(tt.query)

		if tt.err == nil {
			assert.NoError(err, "query %s", tt.query)
		} else {
			assert.Error(err, "query %s", tt.query)
		}

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestKill(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "kill client 824636929312;",
			exp: &spqrparser.Kill{
				Cmd:    spqrparser.ClientStr,
				Target: 824636929312,
			},
			err: nil,
		},

		{
			query: `kill client "824636929312";`,
			exp: &spqrparser.Kill{
				Cmd:    spqrparser.ClientStr,
				Target: 824636929312,
			},
			err: nil,
		},

		{
			query: "kill backend 824636929313;",
			exp: &spqrparser.Kill{
				Cmd:    spqrparser.BackendStr,
				Target: 824636929313,
			},
			err: nil,
		},
	} {
		tmp, err := spqrparser.Parse(tt.query)

		if tt.err == nil {
			assert.NoError(err, "query %s", tt.query)
		} else {
			assert.Error(err, "query %s", tt.query)
		}

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestICP(t *testing.T) {

	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "attach control point 'p1'",
			exp: &spqrparser.InstanceControlPoint{
				Name:   "p1",
				Enable: true,
				A: &spqrparser.ICPointAction{
					Act: "panic",
				},
			},
			err: nil,
		},
		{
			query: "attach control point 'p1' wait 10 seconds",
			exp: &spqrparser.InstanceControlPoint{
				Name:   "p1",
				Enable: true,
				A: &spqrparser.ICPointAction{
					Act: "sleep",

					Timeout: time.Duration(10 * time.Second),
				},
			},
			err: nil,
		},
		{
			query: "attach control point 'p1' wait",
			exp: &spqrparser.InstanceControlPoint{
				Name:   "p1",
				Enable: true,
				A: &spqrparser.ICPointAction{
					Act:     "sleep",
					Timeout: time.Duration(1 * time.Minute),
				},
			},
			err: nil,
		},
		{
			query: "attach control point 'p1' panic",
			exp: &spqrparser.InstanceControlPoint{
				Name:   "p1",
				Enable: true,
				A: &spqrparser.ICPointAction{
					Act: "panic",
				},
			},
			err: nil,
		},
		{
			query: "detach control point 'p1'",
			exp: &spqrparser.InstanceControlPoint{
				Name:   "p1",
				Enable: false,
			},
			err: nil,
		},
	} {
		tmp, err := spqrparser.Parse(tt.query)

		if tt.err == nil {
			assert.NoError(err, "query %s", tt.query)
		} else {
			assert.Error(err, "query %s", tt.query)
		}

		assert.Equal(tt.exp, tmp, "query %s", tt.query)
	}
}

func TestUniqueIndex(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   spqrparser.Statement
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "CREATE UNIQUE INDEX ui1 ON t COLUMN id TYPE integer",
			exp: &spqrparser.Create{
				Element: &spqrparser.UniqueIndexDefinition{
					ID: "ui1",
					TableName: &rfqn.RelationFQN{
						RelationName: "t",
					},
					Column:  "id",
					ColType: "integer",
				},
			},
			err: nil,
		},
		{
			query: "DROP UNIQUE INDEX ui1",
			exp: &spqrparser.Drop{
				Element: &spqrparser.UniqueIndexSelector{
					ID: "ui1",
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
