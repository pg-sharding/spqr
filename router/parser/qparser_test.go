package parser_test

import (
	"testing"

	"github.com/pg-sharding/spqr/router/parser"
	"github.com/stretchr/testify/assert"
)

func TestParseStateAndComment(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   parser.ParseState
		comm  string
		err   error
	}
	p := parser.QParser{}
	for _, tt := range []tcase{
		{
			query: "--ping",
			exp:   parser.ParseStateEmptyQuery{},
			err:   nil,
		},
		{
			query: "-- ping",
			exp:   parser.ParseStateEmptyQuery{},
			err:   nil,
		},
		{
			query: "SELECT 1 -- ping",
			exp:   parser.ParseStateQuery{},
			err:   nil,
		},
		{
			query: "SELECT 1",
			exp:   parser.ParseStateQuery{},
			err:   nil,
		},

		{
			query: "SELECT 1 /* z=x z=x */",
			exp:   parser.ParseStateQuery{},
			comm:  " z=x z=x ",
			err:   nil,
		},

		{
			query: "SELECT /* x= y y = z */ 1 /* z=x z=x */",
			exp:   parser.ParseStateQuery{},
			comm:  " x= y y = z , z=x z=x ",
			err:   nil,
		},
	} {
		parserRes, comm, err := p.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.comm, comm, tt.query)

		assert.Equal(tt.exp, parserRes, tt.query)
	}
}
