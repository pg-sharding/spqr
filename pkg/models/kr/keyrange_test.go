package kr_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/stretchr/testify/assert"
)

// TestGetKRCondition is a unit test function that tests the behavior of the GetKRCondition function.
// It verifies that the generated condition string matches the expected result for different test cases.
// The test cases include various combinations of distributions, distributed relations, key ranges, upper bounds, and prefixes.
// The function uses the assert package to compare the generated condition with the expected result.
func TestGetKRCondition(t *testing.T) {
	assert := assert.New(t)

	for i, c := range []struct {
		ds         *distributions.Distribution
		rel        *distributions.DistributedRelation
		krg        *kr.KeyRange
		upperBound kr.KeyRangeBound
		prefix     string
		expected   string
		err        error
	}{
		{
			ds: &distributions.Distribution{ColTypes: []string{"integer"}},
			rel: &distributions.DistributedRelation{
				Name: "rel",
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "col1", HashFunction: "ident"},
				},
			},
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []interface{}{0}, ColumnTypes: []string{"integer"}},
			upperBound: []interface{}{10},
			prefix:     "",
			expected:   "col1 >= 0 AND col1 < 10",
			err:        nil,
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
			krg: &kr.KeyRange{ID: "kr1", LowerBound: []interface {
			}{
				0,
			},
				ColumnTypes: []string{"integer"},
			},
			upperBound: []interface{}{10},
			prefix:     "rel",
			expected:   "rel.col1 >= 0 AND rel.col1 < 10",
			err:        nil,
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
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []interface{}{0}, ColumnTypes: []string{"integer"}},
			upperBound: nil,
			prefix:     "",
			expected:   "col1 >= 0",
			err:        nil,
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
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []interface{}{"a"}, ColumnTypes: []string{"varchar"}},
			upperBound: []interface{}{"b"},
			prefix:     "",
			expected:   "col1 >= 'a' AND col1 < 'b'",
			err:        nil,
		},
		// city hashed column
		{
			ds: &distributions.Distribution{ColTypes: []string{"varchar"}},
			rel: &distributions.DistributedRelation{
				Name: "rel",
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "col1", HashFunction: "city"},
				},
			},
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []interface{}{"a"}, ColumnTypes: []string{"varchar"}},
			upperBound: []interface{}{"b"},
			prefix:     "",
			expected:   "",
			err:        spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "city hashing is not supported in coordinator operations"),
		},
		// murmur hashed column
		{
			ds: &distributions.Distribution{ColTypes: []string{"varchar"}},
			rel: &distributions.DistributedRelation{
				Name: "rel",
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "col1", HashFunction: "murmur"},
				},
			},
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []interface{}{0}, ColumnTypes: []string{"varchar hashed"}},
			upperBound: []interface{}{1000},
			prefix:     "",
			expected:   "(hash_string(col1, 'murmur3') + 2147483648) >= 0 AND (hash_string(col1, 'murmur3') + 2147483648) < 1000",
			err:        nil,
		},
		// incorrect hash
		{
			ds: &distributions.Distribution{ColTypes: []string{"varchar"}},
			rel: &distributions.DistributedRelation{
				Name: "rel",
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "col1", HashFunction: "nonexistent"},
				},
			},
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []interface{}{"a"}, ColumnTypes: []string{"varchar"}},
			upperBound: []interface{}{"b"},
			prefix:     "",
			expected:   "",
			err:        spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "invalid hash function \"nonexistent\""),
		},
	} {
		cond, err := kr.GetKRCondition(c.rel, c.krg, c.upperBound, c.prefix)
		if c.err != nil {
			assert.EqualError(err, c.err.Error())
		} else {
			assert.NoError(err)
		}
		assert.Equal(
			cond,
			c.expected,
			"test case %d", i,
		)
	}
}
