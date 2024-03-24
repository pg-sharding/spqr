package spqrparser_test

import (
	"testing"

	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/stretchr/testify/assert"
)

func TestSimpleLex(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   []int
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "SHOW version",
			exp:   []int{spqrparser.SHOW, spqrparser.IDENT},
			err:   nil,
		},
		{
			query: "START TRACE ALL MESSAGES",
			exp:   []int{spqrparser.START, spqrparser.TRACE, spqrparser.ALL, spqrparser.MESSAGES},
			err:   nil,
		},
		{
			query: "START TRACE CLIENT 9df3bj3",
			exp:   []int{spqrparser.START, spqrparser.TRACE, spqrparser.CLIENT, spqrparser.IDENT},
			err:   nil,
		},
		{
			query: "STOP TRACE MESSAGES",
			exp:   []int{spqrparser.STOP, spqrparser.TRACE, spqrparser.MESSAGES},
			err:   nil,
		},
		{
			query: "kill client 1234567;",
			exp:   []int{spqrparser.KILL, spqrparser.CLIENT, spqrparser.ICONST},
			err:   nil,
		},
		{
			query: "SHOW clients where user = 'usr1' or dbname = 'db1';",
			exp: []int{
				spqrparser.SHOW, spqrparser.IDENT,
				spqrparser.WHERE,
				spqrparser.IDENT,
				spqrparser.TEQ,
				spqrparser.SCONST,
				spqrparser.OR,
				spqrparser.IDENT,
				spqrparser.TEQ,
				spqrparser.SCONST},
			err: nil,
		},

		{
			query: "ADD KEY RANGE krid2 FROM 88888888-8888-8888-8888-888888888889 TO FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF ROUTE TO sh2;",
			exp: []int{
				spqrparser.ADD,
				spqrparser.KEY,
				spqrparser.RANGE,
				spqrparser.IDENT,
				spqrparser.FROM,
				spqrparser.IDENT,
				spqrparser.TO,
				spqrparser.IDENT,
				spqrparser.ROUTE,
				spqrparser.TO,
				spqrparser.IDENT,
			},
			err: nil,
		},
		{
			query: "ATTACH TABLE t TO DISTRIBUTION ds1;",
			exp: []int{
				spqrparser.ATTACH,
				spqrparser.TABLE,
				spqrparser.IDENT,
				spqrparser.TO,
				spqrparser.DISTRIBUTION,
				spqrparser.IDENT,
			},
		},
		{
			query: "CREATE DISTRIBUTION db1 SHARDING COLUMN TYPES varchar, varchar",
			exp: []int{
				spqrparser.CREATE,
				spqrparser.DISTRIBUTION,
				spqrparser.IDENT,
				spqrparser.SHARDING,
				spqrparser.COLUMN,
				spqrparser.TYPES,
				spqrparser.VARCHAR,
				spqrparser.TCOMMA,
				spqrparser.VARCHAR,
			},
		},
		{
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION t DISTRIBUTION KEY id",
			exp: []int{
				spqrparser.ALTER,
				spqrparser.DISTRIBUTION,
				spqrparser.IDENT,
				spqrparser.ATTACH,
				spqrparser.RELATION,
				spqrparser.IDENT,
				spqrparser.DISTRIBUTION,
				spqrparser.KEY,
				spqrparser.IDENT,
			},
		},
		{
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION t DISTRIBUTION KEY id1, id2",
			exp: []int{
				spqrparser.ALTER,
				spqrparser.DISTRIBUTION,
				spqrparser.IDENT,
				spqrparser.ATTACH,
				spqrparser.RELATION,
				spqrparser.IDENT,
				spqrparser.DISTRIBUTION,
				spqrparser.KEY,
				spqrparser.IDENT,
				spqrparser.TCOMMA,
				spqrparser.IDENT,
			},
		},
		{
			query: "ALTER DISTRIBUTION ds1 DETACH RELATION t",
			exp: []int{
				spqrparser.ALTER,
				spqrparser.DISTRIBUTION,
				spqrparser.IDENT,
				spqrparser.DETACH,
				spqrparser.RELATION,
				spqrparser.IDENT,
			},
		},
		{
			query: "CREATE SHARD sh1 WITH HOSTS localhost:6432",
			exp: []int{
				spqrparser.CREATE,
				spqrparser.SHARD,
				spqrparser.IDENT,
				spqrparser.WITH,
				spqrparser.HOSTS,
				spqrparser.IDENT,
			},
		},
		{
			query: "CREATE SHARD sh1 WITH HOSTS localhost:6432, other_host:7432",
			exp: []int{
				spqrparser.CREATE,
				spqrparser.SHARD,
				spqrparser.IDENT,
				spqrparser.WITH,
				spqrparser.HOSTS,
				spqrparser.IDENT,
				spqrparser.TCOMMA,
				spqrparser.IDENT,
			},
		},
		{
			query: "DROP SHARD sh1",
			exp: []int{
				spqrparser.DROP,
				spqrparser.SHARD,
				spqrparser.IDENT,
			},
		},
	} {
		tmp := spqrparser.NewStringTokenizer(tt.query)

		act := spqrparser.LexString(tmp)

		assert.Equal(tt.exp, act)
	}
}
