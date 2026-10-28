package qparser

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/stretchr/testify/assert"
)

func TestParseComment(t *testing.T) {
	assert := assert.New(t)

	type tmp struct {
		sample string
		exp    map[string]string
		err    error
	}

	for _, tt := range []tmp{
		{
			sample: "lol: kek",
			exp: map[string]string{
				"lol": "kek",
			},
			err: nil,
		},

		{
			sample: "lol kek",
			err:    fmt.Errorf("no colon"),
		},

		{
			sample: "lol: kek lol2: kek2",
			err:    fmt.Errorf("no comma"),
		},

		{
			sample: "vguoyguoygoyy",
			err:    fmt.Errorf("wtf"),
		},

		{
			sample: "lol: kek, lol2 : kek2",
			exp: map[string]string{
				"lol":  "kek",
				"lol2": "kek2",
			},
			err: nil,
		},
		{
			sample: "lol: kek , lol2 : kek2   , lol3:     kek3",
			exp: map[string]string{
				"lol":  "kek",
				"lol2": "kek2",
				"lol3": "kek3",
			},
			err: nil,
		},
		{
			sample: " __spqr__.preferred_engine: v2  ",
			exp: map[string]string{
				"__spqr__preferred_engine": "v2",
			},
			err: nil,
		},
		{
			sample: "lol: kek, lol2 : kek2,   lol3:     kek3    , lol4:kek4   ,  lol5 :kek5",
			exp: map[string]string{
				"lol":  "kek",
				"lol2": "kek2",
				"lol3": "kek3",
				"lol4": "kek4",
				"lol5": "kek5",
			},
			err: nil,
		},
		{
			sample: "random comment in random format , __spqr__execute_on: sh3 ",
			exp:    nil,
			err:    spqrerror.Newf(spqrerror.SPQR_UNEXPECTED, "invalid comment format: expected colon after option name"),
		},
		// values with spaces are not supported (no quoting mechanism)
		{
			sample: "lol: hello world",
			exp:    nil,
			err:    spqrerror.Newf(spqrerror.SPQR_UNEXPECTED, "invalid comment format: expected comma after not-last key-value pair"),
		},
		{
			sample: "lol: hello world, lol2: kek",
			exp:    nil,
			err:    spqrerror.Newf(spqrerror.SPQR_UNEXPECTED, "invalid comment format: expected comma after not-last key-value pair"),
		},
	} {

		mp, err := ParseComment(tt.sample)

		if tt.err != nil {
			assert.Error(err)
		} else {
			assert.NoError(err)
			assert.Equal(tt.exp, mp)
		}
	}
}

func buildComment(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "__spqr__key%d: value%d", i, i)
	}
	return b.String()
}

func buildCommentLongValues(n, valLen int) string {
	val := strings.Repeat("x", valLen)
	var b strings.Builder
	b.Grow(n * (16 + valLen))
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "__spqr__key%d: %s", i, val)
	}
	return b.String()
}

func BenchmarkParseComment(b *testing.B) {
	for _, tt := range []struct {
		name    string
		comment string
	}{
		{
			name:    "15k-keys",
			comment: buildComment(15000),
		},
		{
			name:    "long-value/100-keys/1k",
			comment: buildCommentLongValues(100, 1024),
		},
		{
			name:    "long-values/20-keys/10k",
			comment: buildCommentLongValues(20, 10240),
		},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.SetBytes(int64(len(tt.comment)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _ = ParseComment(tt.comment)
			}
		})
	}
}
