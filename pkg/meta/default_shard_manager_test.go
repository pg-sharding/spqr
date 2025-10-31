package meta_test

import (
	"math"
	"testing"

	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/stretchr/testify/assert"
)

func TestDefaultRangeLowerBound(t *testing.T) {

	type tcase struct {
		colTypes    []string
		expected    kr.KeyRangeBound
		expectedErr error
	}
	for _, test := range []tcase{
		{
			colTypes: []string{"varchar",
				"integer"},
			expected:    kr.KeyRangeBound{"", int64(math.MinInt64)},
			expectedErr: nil,
		},
		{
			colTypes: []string{"varchar",
				"integer",
				"uuid",
			},
			expected:    kr.KeyRangeBound{"", int64(math.MinInt64), "00000000-0000-0000-0000-000000000000"},
			expectedErr: nil,
		},
	} {
		actual, actualErr := meta.DefaultRangeLowerBound(test.colTypes)

		if test.expectedErr == nil {
			assert.NoError(t, actualErr, "type set %v", test.colTypes)
			assert.ElementsMatch(t, test.expected, actual)
		} else {
			assert.Error(t, actualErr, "type set %v", test.colTypes)
			assert.Nil(t, actual)
		}
	}
}

func TestCmpRangesLess_Default(t *testing.T) {
	defaultString, _ := meta.DefaultRangeLowerBound([]string{"varchar"})
	defaultInteger, _ := meta.DefaultRangeLowerBound([]string{"integer"})
	defaultMix1, _ := meta.DefaultRangeLowerBound([]string{"integer", "varchar"})
	defaultMix2, _ := meta.DefaultRangeLowerBound([]string{"varchar", "integer"})
	defaultMix3, _ := meta.DefaultRangeLowerBound([]string{"integer", "varchar", "integer"})
	tests := []struct {
		testName   string
		leftBound  kr.KeyRangeBound
		checkBound kr.KeyRangeBound
		types      []string
		expect     bool
	}{
		{
			testName:   "10",
			leftBound:  defaultInteger,
			checkBound: kr.KeyRangeBound{int64(10)},
			types:      []string{"integer"},
			expect:     true,
		},
		{
			testName:   "-10",
			leftBound:  defaultInteger,
			checkBound: kr.KeyRangeBound{int64(-10)},
			types:      []string{"integer"},
			expect:     true,
		},
		{
			testName:   "-10 inverted test",
			leftBound:  kr.KeyRangeBound{int64(-10)},
			checkBound: defaultInteger,
			types:      []string{"integer"},
			expect:     false,
		},
		{
			testName:   "-9223372036854775807",
			leftBound:  defaultInteger,
			checkBound: kr.KeyRangeBound{int64(math.MinInt64 + 1)},
			types:      []string{"integer"},
			expect:     true,
		},
		{
			testName:   "empty too",
			leftBound:  defaultString,
			checkBound: kr.KeyRangeBound{""},
			types:      []string{"varchar"},
			expect:     false,
		},
		{
			testName:   "test1",
			leftBound:  defaultString,
			checkBound: kr.KeyRangeBound{"test1"},
			types:      []string{"varchar"},
			expect:     true,
		},
		{
			testName:   "minInt, tt",
			leftBound:  defaultMix1,
			checkBound: kr.KeyRangeBound{int64(math.MinInt64), "test1"},
			types:      []string{"integer", "varchar"},
			expect:     true,
		},
		{
			testName:   "minInt, <empty>",
			leftBound:  defaultMix1,
			checkBound: kr.KeyRangeBound{int64(math.MinInt64), ""},
			types:      []string{"integer", "varchar"},
			expect:     false,
		},
		{
			testName:   "minInt+1, <empty>",
			leftBound:  defaultMix1,
			checkBound: kr.KeyRangeBound{int64(math.MinInt64 + 1), ""},
			types:      []string{"integer", "varchar"},
			expect:     true,
		},
		{
			testName:   "<empty>,minInt+1",
			leftBound:  defaultMix2,
			checkBound: kr.KeyRangeBound{"", int64(math.MinInt64 + 1)},
			types:      []string{"varchar", "integer"},
			expect:     true,
		},
		{
			testName:   "<empty>,minInt+1  inverted test",
			leftBound:  kr.KeyRangeBound{"", int64(math.MinInt64 + 1)},
			checkBound: defaultMix2,
			types:      []string{"varchar", "integer"},
			expect:     false,
		},
		{
			testName:   "minInt+1,t, minInt",
			leftBound:  defaultMix3,
			checkBound: kr.KeyRangeBound{int64(math.MinInt64 + 1), "", int64(math.MinInt64)},
			types:      []string{"integer", "varchar", "integer"},
			expect:     true,
		},
	}

	for _, test := range tests {
		result := kr.CmpRangesLess(test.leftBound, test.checkBound, test.types)
		assert.Equal(t, test.expect, result, test.testName)
	}
}
