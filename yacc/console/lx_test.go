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
			query: `START TRACE CLIENT "9df3bj3"`,
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
			exp:   []int{spqrparser.KILL, spqrparser.CLIENT, spqrparser.ICONST, spqrparser.TSEMICOLON},
			err:   nil,
		},

		{
			query: "SHOW clients where user = 'usr1' or dbname = 'db1';",
			exp: []int{
				spqrparser.SHOW, spqrparser.CLIENTS,
				spqrparser.WHERE,
				spqrparser.IDENT,
				spqrparser.TEQ,
				spqrparser.SCONST,
				spqrparser.OR,
				spqrparser.IDENT,
				spqrparser.TEQ,
				spqrparser.SCONST,
				spqrparser.TSEMICOLON,
			},
			err: nil,
		},

		{
			query: `SHOW clients where user = 'usr1' or "dbname" = 'db1';`,
			exp: []int{
				spqrparser.SHOW, spqrparser.CLIENTS,
				spqrparser.WHERE,
				spqrparser.IDENT,
				spqrparser.TEQ,
				spqrparser.SCONST,
				spqrparser.OR,
				spqrparser.IDENT,
				spqrparser.TEQ,
				spqrparser.SCONST,
				spqrparser.TSEMICOLON,
			},
			err: nil,
		},

		{
			query: `SHOW relations WHERE "Distribution ID" = 'ds1';`,
			exp: []int{
				spqrparser.SHOW, spqrparser.RELATIONS,
				spqrparser.WHERE,
				spqrparser.IDENT,
				spqrparser.TEQ,
				spqrparser.SCONST,
				spqrparser.TSEMICOLON,
			},
			err: nil,
		},

		{
			query: "ADD KEY RANGE krid2 FROM '88888888-8888-8888-8888-888888888889' ROUTE TO sh2;",
			exp: []int{
				spqrparser.ADD,
				spqrparser.KEY,
				spqrparser.RANGE,
				spqrparser.IDENT,
				spqrparser.FROM,
				spqrparser.SCONST,
				spqrparser.ROUTE,
				spqrparser.TO,
				spqrparser.IDENT,
				spqrparser.TSEMICOLON,
			},
			err: nil,
		},
		{
			query: "CREATE KEY RANGE krid2 FROM - 18 ROUTE TO sh2;",
			exp: []int{
				spqrparser.CREATE,
				spqrparser.KEY,
				spqrparser.RANGE,
				spqrparser.IDENT,
				spqrparser.FROM,
				spqrparser.TMINUS,
				spqrparser.ICONST,
				spqrparser.ROUTE,
				spqrparser.TO,
				spqrparser.IDENT,
				spqrparser.TSEMICOLON,
			},
			err: nil,
		},
		{
			query: `CREATE KEY RANGE krid1 FROM 1 ROUTE TO 'sh-1' FOR DISTRIBUTION ds1;`,
			exp: []int{
				spqrparser.CREATE,
				spqrparser.KEY,
				spqrparser.RANGE,
				spqrparser.IDENT,
				spqrparser.FROM,
				spqrparser.ICONST,
				spqrparser.ROUTE,
				spqrparser.TO,
				spqrparser.SCONST,
				spqrparser.FOR,
				spqrparser.DISTRIBUTION,
				spqrparser.IDENT,
				spqrparser.TSEMICOLON,
			},
			err: nil,
		},
		{
			query: "CREATE KEY RANGE krid2 FROM -18 ROUTE TO sh2;",
			exp: []int{
				spqrparser.CREATE,
				spqrparser.KEY,
				spqrparser.RANGE,
				spqrparser.IDENT,
				spqrparser.FROM,
				spqrparser.TMINUS,
				spqrparser.ICONST,
				spqrparser.ROUTE,
				spqrparser.TO,
				spqrparser.IDENT,
				spqrparser.TSEMICOLON,
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
				spqrparser.TSEMICOLON,
			},
		},
		{
			query: "CREATE DISTRIBUTION db1 COLUMN TYPES varchar, varchar, uuid",
			exp: []int{
				spqrparser.CREATE,
				spqrparser.DISTRIBUTION,
				spqrparser.IDENT,
				spqrparser.COLUMN,
				spqrparser.TYPES,
				spqrparser.VARCHAR,
				spqrparser.TCOMMA,
				spqrparser.VARCHAR,
				spqrparser.TCOMMA,
				spqrparser.UUID,
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
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION sch1.t DISTRIBUTION KEY id",
			exp: []int{
				spqrparser.ALTER,
				spqrparser.DISTRIBUTION,
				spqrparser.IDENT,
				spqrparser.ATTACH,
				spqrparser.RELATION,
				spqrparser.IDENT,
				spqrparser.TDOT,
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
			query: "ALTER DISTRIBUTION ds1 ATTACH RELATION t DISTRIBUTION KEY id1, id2 AUTO INCREMENT id1, id3",
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
				spqrparser.AUTO,
				spqrparser.INCREMENT,
				spqrparser.IDENT,
				spqrparser.TCOMMA,
				spqrparser.IDENT,
			},
		},
		{
			query: `CREATE DISTRIBUTED RELATION t (HASH MURMUR [id1 INT, id2 VARCHAR]);`,
			exp: []int{
				spqrparser.CREATE,
				spqrparser.DISTRIBUTED,
				spqrparser.RELATION,
				spqrparser.IDENT,
				spqrparser.TOPENBR,
				spqrparser.HASH,
				spqrparser.MURMUR,
				spqrparser.TOPENSQBR,
				spqrparser.IDENT,
				spqrparser.INT,
				spqrparser.TCOMMA,
				spqrparser.IDENT,
				spqrparser.VARCHAR,
				spqrparser.TCLOSESQBR,
				spqrparser.TCLOSEBR,
				spqrparser.TSEMICOLON,
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
			query: `CREATE SHARD sh1 WITH HOSTS "localhost:6432"`,
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
			query: `CREATE SHARD sh1 WITH HOSTS "localhost:6432", "other_host:7432"`,
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
		{
			query: "DROP TASK GROUP",
			exp: []int{
				spqrparser.DROP,
				spqrparser.TASK,
				spqrparser.GROUP,
			},
		},
		{
			query: "INVALIDATE CACHE",
			exp: []int{
				spqrparser.INVALIDATE,
				spqrparser.CACHE,
			},
		},
		{
			query: "DROP SEQUENCE seq",
			exp: []int{
				spqrparser.DROP,
				spqrparser.SEQUENCE,
				spqrparser.IDENT,
			},
		},
		{
			query: "DROP SEQUENCE seq",
			exp: []int{
				spqrparser.DROP,
				spqrparser.SEQUENCE,
				spqrparser.IDENT,
			},
		},
		{
			query: "CREATE DISTRIBUTION ds1 DEFAULT shard1",
			exp: []int{
				spqrparser.CREATE,
				spqrparser.DISTRIBUTION,
				spqrparser.IDENT,
				spqrparser.DEFAULT,
				spqrparser.IDENT,
			},
		},
		{
			query: "CREATE DISTRIBUTION ds1 COLUMN TYPES integer DEFAULT shard1",
			exp: []int{
				spqrparser.CREATE,
				spqrparser.DISTRIBUTION,
				spqrparser.IDENT,
				spqrparser.COLUMN,
				spqrparser.TYPES,
				spqrparser.INTEGER,
				spqrparser.DEFAULT,
				spqrparser.IDENT,
			},
		},
		{
			query: "CREATE UNIQUE INDEX ui1 ON t COLUMN sec_id TYPE integer",
			exp: []int{
				spqrparser.CREATE,
				spqrparser.UNIQUE,
				spqrparser.INDEX,
				spqrparser.IDENT,
				spqrparser.ON,
				spqrparser.IDENT,
				spqrparser.COLUMN,
				spqrparser.IDENT,
				spqrparser.TYPE,
				spqrparser.INTEGER,
			},
		},
	} {
		tmp := spqrparser.NewStringTokenizer(tt.query)

		act := spqrparser.LexString(tmp)

		assert.Equal(tt.exp, act, tt.query)
	}
}

func TestDefaultShard(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   []int
	}

	for _, tt := range []tcase{
		{
			query: "CREATE DISTRIBUTION db1 DEFAULT shard1",
			exp: []int{
				spqrparser.CREATE,
				spqrparser.DISTRIBUTION,
				spqrparser.IDENT,
				spqrparser.DEFAULT,
				spqrparser.IDENT,
			},
		},
		{
			query: "ALTER DISTRIBUTION distr1 DROP DEFAULT SHARD",
			exp: []int{
				spqrparser.ALTER,
				spqrparser.DISTRIBUTION,
				spqrparser.IDENT,
				spqrparser.DROP,
				spqrparser.DEFAULT,
				spqrparser.SHARD,
			},
		},
	} {
		tmp := spqrparser.NewStringTokenizer(tt.query)

		act := spqrparser.LexString(tmp)

		assert.Equal(tt.exp, act)
	}
}

func TestIncorrectNumbers(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   []int
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "SHOW -9223372036854775807",
			exp:   []int{spqrparser.SHOW, spqrparser.TMINUS, spqrparser.ICONST},
			err:   nil,
		},
		{
			query: "SHOW 18446744073709551616 222",
			exp:   []int{spqrparser.SHOW, spqrparser.INVALID_ICONST, spqrparser.ICONST},
			err:   nil,
		},
		{
			query: "SHOW 9223372036854775807 222",
			exp:   []int{spqrparser.SHOW, spqrparser.ICONST, spqrparser.ICONST},
			err:   nil,
		},
		{
			query: "SHOW -18446744073709551616 -222",
			exp: []int{spqrparser.SHOW,
				spqrparser.TMINUS,
				spqrparser.INVALID_ICONST,
				spqrparser.TMINUS,
				spqrparser.ICONST},
			err: nil,
		},
	} {
		tmp := spqrparser.NewStringTokenizer(tt.query)

		act := spqrparser.LexString(tmp)

		assert.Equal(tt.exp, act, tt.query)
	}
}
func TestQualifiedNames(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   []int
		err   error
	}

	for _, tt := range []tcase{
		{
			query: "ALTER DISTRIBUTION ds1 DETACH RELATION schema1.t",
			exp: []int{spqrparser.ALTER,
				spqrparser.DISTRIBUTION,
				spqrparser.IDENT,
				spqrparser.DETACH,
				spqrparser.RELATION,
				spqrparser.IDENT,
				spqrparser.TDOT,
				spqrparser.IDENT,
			},
			err: nil,
		},
	} {
		tmp := spqrparser.NewStringTokenizer(tt.query)

		act := spqrparser.LexString(tmp)

		assert.Equal(tt.exp, act, tt.query)
	}
}
