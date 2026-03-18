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

func TestCheckDuplicateKeyColumns(t *testing.T) {
	assert := assert.New(t)

	assert.NoError(distributions.CheckDuplicateKeyColumns([]distributions.DistributionKeyEntry{
		{Column: "a", HashFunction: "identity"},
		{Column: "b", HashFunction: "identity"},
	}))

	err := distributions.CheckDuplicateKeyColumns([]distributions.DistributionKeyEntry{
		{Column: "a", HashFunction: "identity"},
		{Column: "a", HashFunction: "identity"},
	})
	assert.Error(err)
	assert.ErrorContains(err, "duplicate column \"a\"")

	assert.NoError(distributions.CheckDuplicateKeyColumns([]distributions.DistributionKeyEntry{
		{Column: "", HashFunction: "murmur"},
		{Column: "", HashFunction: "murmur"},
	}), "expression entries with empty Column should not trigger duplicates")

	assert.NoError(distributions.CheckDuplicateKeyColumns([]distributions.DistributionKeyEntry{}))
}

func TestRenameKeyColumnDuplicate(t *testing.T) {
	rel := &distributions.DistributedRelation{
		Relation: &rfqn.RelationFQN{RelationName: "t"},
		DistributionKey: []distributions.DistributionKeyEntry{
			{Column: "a", HashFunction: "identity"},
			{Column: "b", HashFunction: "identity"},
		},
	}

	newKey, err := rel.RenameKeyColumn("a", "c")
	assert.NoError(t, err)
	assert.Equal(t, "c", newKey[0].Column)
	assert.Equal(t, "b", newKey[1].Column)

	_, err = rel.RenameKeyColumn("nonexistent", "x")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "not found in distribution key")
}

func TestRenameKeyColumnUniqueIndex(t *testing.T) {
	rel := &distributions.DistributedRelation{
		Relation: &rfqn.RelationFQN{RelationName: "t"},
		DistributionKey: []distributions.DistributionKeyEntry{
			{Column: "a", HashFunction: "identity"},
		},
		UniqueIndexesByColumn: map[string]*distributions.UniqueIndex{
			"a": {ID: "idx1", Columns: []string{"a"}},
		},
	}

	_, err := rel.RenameKeyColumn("a", "b")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "cannot rename column \"a\": referenced by a unique index")
}

func TestRenameKeyColumnExpressionOnly(t *testing.T) {
	rel := &distributions.DistributedRelation{
		Relation: &rfqn.RelationFQN{RelationName: "t"},
		DistributionKey: []distributions.DistributionKeyEntry{
			{Column: "", HashFunction: "murmur", Expr: distributions.RoutingExpr{
				ColRefs: []distributions.TypedColRef{{ColName: "id1", ColType: "int"}},
			}},
		},
	}

	_, err := rel.RenameKeyColumn("id1", "new_id")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "expression-based routing; column rename is not supported")
}

func TestRenameKeyColumnMixedColumnAndExpression(t *testing.T) {
	rel := &distributions.DistributedRelation{
		Relation: &rfqn.RelationFQN{RelationName: "t"},
		DistributionKey: []distributions.DistributionKeyEntry{
			{Column: "tenant_id", HashFunction: "identity"},
			{Column: "", HashFunction: "murmur", Expr: distributions.RoutingExpr{
				ColRefs: []distributions.TypedColRef{{ColName: "id1", ColType: "int"}},
			}},
		},
	}

	newKey, err := rel.RenameKeyColumn("tenant_id", "site_id")
	assert.NoError(t, err)
	assert.Equal(t, "site_id", newKey[0].Column)
	assert.Equal(t, "", newKey[1].Column, "expression entry should be untouched")
	assert.Len(t, newKey[1].Expr.ColRefs, 1, "expression ColRefs should be preserved")

	_, err = rel.RenameKeyColumn("id1", "new_id")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "not found in distribution key",
		"column names inside Expr.ColRefs cannot be renamed via this command")
}
