package parser

import (
	"fmt"
	"testing"

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
