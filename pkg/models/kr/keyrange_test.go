package kr_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/qdb"
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
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []any{0}, ColumnTypes: []string{"integer"}},
			upperBound: []any{10},
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
			upperBound: []any{10},
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
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []any{0}, ColumnTypes: []string{"integer"}},
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
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []any{"a"}, ColumnTypes: []string{"varchar"}},
			upperBound: []any{"b"},
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
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []any{"a"}, ColumnTypes: []string{"varchar"}},
			upperBound: []any{"b"},
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
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []any{0}, ColumnTypes: []string{"varchar hashed"}},
			upperBound: []any{1000},
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
			krg:        &kr.KeyRange{ID: "kr1", LowerBound: []any{"a"}, ColumnTypes: []string{"varchar"}},
			upperBound: []any{"b"},
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
			c.expected,
			cond,
			"test case %d", i,
		)
	}
}

func TestCmpRangesLess_UInteger(t *testing.T) {
	tests := []struct {
		name   string
		bound  kr.KeyRangeBound
		key    kr.KeyRangeBound
		types  []string
		expect bool
	}{
		{
			name:   "uint64: bound < key",
			bound:  kr.KeyRangeBound{uint64(10)},
			key:    kr.KeyRangeBound{uint64(20)},
			types:  []string{qdb.ColumnTypeUinteger},
			expect: true,
		},
		{
			name:   "uint64: bound > key",
			bound:  kr.KeyRangeBound{uint64(30)},
			key:    kr.KeyRangeBound{uint64(20)},
			types:  []string{qdb.ColumnTypeUinteger},
			expect: false,
		},
		{
			name:   "uint64: bound == key",
			bound:  kr.KeyRangeBound{uint64(20)},
			key:    kr.KeyRangeBound{uint64(20)},
			types:  []string{qdb.ColumnTypeUinteger},
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := kr.CmpRangesLess(tt.bound, tt.key, tt.types)
			assert.Equal(t, tt.expect, result)
		})
	}
}

func TestCmpRangesLess_Integer(t *testing.T) {
	tests := []struct {
		name   string
		bound  kr.KeyRangeBound
		key    kr.KeyRangeBound
		types  []string
		expect bool
	}{
		{
			name:   "int64: bound < key (negative)",
			bound:  kr.KeyRangeBound{int64(-10)},
			key:    kr.KeyRangeBound{int64(10)},
			types:  []string{qdb.ColumnTypeInteger},
			expect: true,
		},
		{
			name:   "int64: bound > key",
			bound:  kr.KeyRangeBound{int64(30)},
			key:    kr.KeyRangeBound{int64(20)},
			types:  []string{qdb.ColumnTypeInteger},
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := kr.CmpRangesLess(tt.bound, tt.key, tt.types)
			assert.Equal(t, tt.expect, result)
		})
	}
}

func TestCmpRangesLess_String(t *testing.T) {
	tests := []struct {
		name   string
		bound  kr.KeyRangeBound
		key    kr.KeyRangeBound
		types  []string
		expect bool
	}{
		{
			name:   "string: bound < key",
			bound:  kr.KeyRangeBound{"apple"},
			key:    kr.KeyRangeBound{"banana"},
			types:  []string{qdb.ColumnTypeVarchar},
			expect: true,
		},
		{
			name:   "string: bound > key",
			bound:  kr.KeyRangeBound{"zebra"},
			key:    kr.KeyRangeBound{"apple"},
			types:  []string{qdb.ColumnTypeVarchar},
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := kr.CmpRangesLess(tt.bound, tt.key, tt.types)
			assert.Equal(t, tt.expect, result)
		})
	}
}

func TestCmpRangesLess_DeprecatedString(t *testing.T) {
	tests := []struct {
		name   string
		bound  kr.KeyRangeBound
		key    kr.KeyRangeBound
		types  []string
		expect bool
	}{
		{
			name:   "deprecated string: custom comparison",
			bound:  kr.KeyRangeBound{"a"},
			key:    kr.KeyRangeBound{"b"},
			types:  []string{qdb.ColumnTypeVarcharDeprecated},
			expect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := kr.CmpRangesLess(tt.bound, tt.key, tt.types)
			assert.Equal(t, tt.expect, result)
		})
	}
}

func TestCmpRangesLess_PanicOnTypeMismatch(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic on type mismatch")
		}
	}()

	bound := kr.KeyRangeBound{"not-a-number"}
	key := kr.KeyRangeBound{uint64(10)}
	types := []string{qdb.ColumnTypeUinteger}

	kr.CmpRangesLess(bound, key, types)
}
