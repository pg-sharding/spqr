package qparser_test

import (
	"testing"

	"github.com/pg-sharding/spqr/router/qparser"
	"github.com/stretchr/testify/assert"
)

func TestQParser(t *testing.T) {

	assert := assert.New(t)

	type tmp struct {
		sample string
		exp    []string
		err    error
	}

	for _, tt := range []tmp{
		{
			sample: "select /* random comment in random format */ 6 + 7",
			exp:    []string{" random comment in random format "},
			err:    nil,
		},
		{
			sample: "select /* random comment in random format */ 6 + 7  /* __spqr__execute_on: sh3 */",
			exp:    []string{" random comment in random format ", " __spqr__execute_on: sh3 "},
			err:    nil,
		},

		{
			sample: "select /* __spqr__engine_v2: false  */ 6 + 7  /* __spqr__execute_on: sh3 */",
			exp:    []string{" __spqr__engine_v2: false  ", " __spqr__execute_on: sh3 "},
			err:    nil,
		},
	} {

		qp := &qparser.QParser{}

		_, comments, err := qp.Parse(tt.sample)
		if tt.err != nil {
			assert.Error(err)
		} else {
			assert.NoError(err)
			assert.Equal(tt.exp, comments)
		}
	}
}
