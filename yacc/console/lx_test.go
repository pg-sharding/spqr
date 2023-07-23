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
	} {
		tmp := spqrparser.NewStringTokenizer(tt.query)

		act := spqrparser.LexString(tmp)

		assert.Equal(tt.exp, act)
	}
}
