package parser_test

import (
	"testing"

	"github.com/pg-sharding/spqr/router/parser"
	"github.com/stretchr/testify/assert"
)

func TestCommentParsing(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		query string
		exp   parser.ParseState
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
	} {
		parserRes, _, err := p.Parse(tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.NoError(err, "query %s", tt.query)

		assert.Equal(tt.exp, parserRes)
	}
}
