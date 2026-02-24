package distributions_test

import (
	"fmt"
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rfqn"
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
			expected: "spqrhash_murmur3(a)",
			err:      nil,
		},
		{
			col:      "a",
			hash:     "city",
			expected: "spqrhash_city32(a)",
			err:      nil,
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
			expected: []string{"spqrhash_murmur3(a)", "b"},
			err:      nil,
		},
		{
			rel: &distributions.DistributedRelation{
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "a", HashFunction: "murmur"},
					{Column: "b", HashFunction: "city"},
				},
			},
			expected: []string{"spqrhash_murmur3(a)", "spqrhash_city32(b)"},
			err:      nil,
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

func TestHashableKeyChecks(t *testing.T) {
	assert := assert.New(t)
	distribution1Hashed := qdb.NewDistribution("ds1", []string{qdb.ColumnTypeUinteger})
	distribution2Hashed := qdb.NewDistribution("ds1", []string{qdb.ColumnTypeVarchar, qdb.ColumnTypeUinteger})
	for i, tt := range []struct {
		distribution *qdb.Distribution
		rel          *distributions.DistributedRelation
		err          error
	}{
		{
			distribution: distribution1Hashed,
			rel: &distributions.DistributedRelation{
				Relation: &rfqn.RelationFQN{
					RelationName: "r1",
				},
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "a", HashFunction: "ident"},
				},
			},
			err: nil,
		},
		{
			distribution: distribution1Hashed,
			rel: &distributions.DistributedRelation{
				Relation: &rfqn.RelationFQN{
					RelationName: "r1",
					SchemaName:   "public",
				},
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "a"},
				},
			},
			err: fmt.Errorf("hashed type uinteger of distribution ds1 needs hashfunction to attach public.r1"),
		},
		{
			distribution: distribution2Hashed,
			rel: &distributions.DistributedRelation{
				Relation: &rfqn.RelationFQN{
					RelationName: "r1",
				},
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "b"},
					{Column: "a", HashFunction: "ident"},
				},
			},
			err: nil,
		},
		{
			distribution: distribution2Hashed,
			rel: &distributions.DistributedRelation{
				Relation: &rfqn.RelationFQN{
					RelationName: "r1",
					SchemaName:   "public",
				},
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "b", HashFunction: "ident"},
					{Column: "a"},
				},
			},
			err: fmt.Errorf("type varchar of distribution ds1 does not support hashfunction to attach relation public.r1"),
		},
	} {
		actual := distributions.CheckRelationKeys(tt.distribution, tt.rel)
		if tt.err != nil {
			assert.EqualError(actual, tt.err.Error(), fmt.Sprintf("case: %d", i))
		} else {
			assert.NoError(actual)
		}
	}
}
