package distributions_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/stretchr/testify/assert"
)

func TestGetHashedColumn(t *testing.T) {
	assert := assert.New(t)
	for i, c := range []struct {
		col      string
		hash     string
		expected string
		err      error
	}{
		{
			col:      "a",
			hash:     "",
			expected: "a",
			err:      nil,
		},
		{
			col:      "a",
			hash:     "ident",
			expected: "a",
			err:      nil,
		},
		{
			col:      "a",
			hash:     "identity",
			expected: "a",
			err:      nil,
		},
		{
			col:      "a",
			hash:     "murmur",
			expected: "hash_string(a, 'murmur3')",
			err:      nil,
		},
		{
			col:      "a",
			hash:     "city",
			expected: "",
			err:      spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "city hashing is not supported in coordinator operations"),
		},
		{
			col:      "a",
			hash:     "invalid",
			expected: "",
			err:      spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "invalid hash function \"invalid\""),
		},
	} {
		cond, err := distributions.GetHashedColumn(c.col, c.hash)
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

func TestGetDistributionKeyColumns(t *testing.T) {
	assert := assert.New(t)
	for i, c := range []struct {
		rel      *distributions.DistributedRelation
		expected []string
		err      error
	}{
		{
			rel: &distributions.DistributedRelation{
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "a", HashFunction: "ident"},
				},
			},
			expected: []string{"a"},
			err:      nil,
		},
		{
			rel: &distributions.DistributedRelation{
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "a", HashFunction: "ident"},
					{Column: "b", HashFunction: "ident"},
				},
			},
			expected: []string{"a", "b"},
			err:      nil,
		},
		{
			rel: &distributions.DistributedRelation{
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "a", HashFunction: "murmur"},
					{Column: "b", HashFunction: "ident"},
				},
			},
			expected: []string{"hash_string(a, 'murmur3')", "b"},
			err:      nil,
		},
		{
			rel: &distributions.DistributedRelation{
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "a", HashFunction: "murmur"},
					{Column: "b", HashFunction: "city"},
				},
			},
			expected: nil,
			err:      spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "city hashing is not supported in coordinator operations"),
		},
		{
			rel: &distributions.DistributedRelation{
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "a", HashFunction: "murmur"},
					{Column: "b", HashFunction: "invalid"},
				},
			},
			expected: nil,
			err:      spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "invalid hash function \"invalid\""),
		},
	} {
		cond, err := c.rel.GetDistributionKeyColumns()
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
