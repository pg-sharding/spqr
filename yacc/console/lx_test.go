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
			query: "kill client 0xc00030f520;",
			exp:   []int{spqrparser.KILL, spqrparser.IDENT, spqrparser.IDENT},
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
	} {
		tmp := spqrparser.NewStringTokenizer(tt.query)

		act := spqrparser.LexString(tmp)

		assert.Equal(tt.exp, act)
	}
}
