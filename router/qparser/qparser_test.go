package qparser_test

import (
	"fmt"
	"strings"
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

func buildQueryWithComments(nComments int, bodyLen int) string {
	body := strings.Repeat("x", bodyLen)
	var b strings.Builder
	b.WriteString("SELECT ")
	for i := 0; i < nComments; i++ {
		fmt.Fprintf(&b, "/* __spqr__key%d: value%d */ ", i, i)
	}
	b.WriteString(body)
	return b.String()
}

func BenchmarkQParserParse(b *testing.B) {
	for _, tt := range []struct {
		name  string
		query string
	}{
		{
			name:  "one-comment",
			query: "SELECT /* __spqr__execute_on: sh1 */ id FROM t WHERE id = 42",
		},
		{
			name:  "many-comments",
			query: buildQueryWithComments(500, 32),
		},
		{
			name:  "long-query/one-comment",
			query: buildQueryWithComments(1, 4096),
		},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.SetBytes(int64(len(tt.query)))
			b.ReportAllocs()
			b.ResetTimer()
			qp := &qparser.QParser{}
			for b.Loop() {
				_, _, _ = qp.Parse(tt.query)
			}
		})
	}
}
