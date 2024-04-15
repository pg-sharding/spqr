package kr_test

import (
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetKRCondition(t *testing.T) {
	assert := assert.New(t)

	for i, c := range []struct {
		ds         *distributions.Distribution
		rel        *distributions.DistributedRelation
		krg        *kr.KeyRange
		upperBound kr.KeyRangeBound
		prefix     string
		expected   string
	}{
		{
			ds: &distributions.Distribution{ColTypes: []string{"integer"}},
			rel: &distributions.DistributedRelation{
				Name: "rel",
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "col1", HashFunction: "ident"},
				},
			},
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []byte("0")},
			upperBound: []byte("10"),
			prefix:     "",
			expected:   "col1 >= 0 AND col1 < 10",
		},
		// prefix
		{
			ds: &distributions.Distribution{ColTypes: []string{"integer"}},
			rel: &distributions.DistributedRelation{
				Name: "rel",
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "col1", HashFunction: "ident"},
				},
			},
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []byte("0")},
			upperBound: []byte("10"),
			prefix:     "rel",
			expected:   "rel.col1 >= 0 AND rel.col1 < 10",
		},
		// no upper bound
		{
			ds: &distributions.Distribution{ColTypes: []string{"integer"}},
			rel: &distributions.DistributedRelation{
				Name: "rel",
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "col1", HashFunction: "ident"},
				},
			},
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []byte("0")},
			upperBound: nil,
			prefix:     "",
			expected:   "col1 >= 0",
		},
		// string columns
		{
			ds: &distributions.Distribution{ColTypes: []string{"varchar"}},
			rel: &distributions.DistributedRelation{
				Name: "rel",
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "col1", HashFunction: "ident"},
				},
			},
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []byte("a")},
			upperBound: []byte("b"),
			prefix:     "",
			expected:   "col1 >= 'a' AND col1 < 'b'",
		},
	} {
		assert.Equal(
			kr.GetKRCondition(c.ds, c.rel, c.krg, c.upperBound, c.prefix),
			c.expected,
			"test case %d", i,
		)
	}

}
