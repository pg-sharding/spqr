package qdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIdRanges(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		name      string
		rangeLeft int64
		rangeSize uint64
		expected  *SequenceIdRange
		err       error
	}

	for _, tt := range []tcase{
		{
			name:      "test0",
			rangeLeft: 0,
			rangeSize: 1,
			expected:  &SequenceIdRange{Left: 0, Right: 0},
			err:       nil,
		},
		{
			name:      "test1",
			rangeLeft: 1,
			rangeSize: 1,
			expected:  &SequenceIdRange{Left: 1, Right: 1},
			err:       nil,
		},
		{
			name:      "test2",
			rangeLeft: 1,
			rangeSize: 2,
			expected:  &SequenceIdRange{Left: 1, Right: 2},
			err:       nil,
		},
		{
			name:      "test3",
			rangeLeft: 9223372036854775707,
			rangeSize: 300,
			expected:  nil,
			err:       fmt.Errorf("invalid (case 1) id-range request: current=%d, request for=%d", 9223372036854775707, 300),
		},
		{
			name:      "test4",
			rangeLeft: 300,
			rangeSize: 9223372036854775707,
			expected:  nil,
			err:       fmt.Errorf("invalid (case 1) id-range request: current=%d, request for=%d", 300, 9223372036854775707),
		},
		{
			name:      "test5",
			rangeLeft: 9223372036854775707,
			rangeSize: 50,
			expected:  &SequenceIdRange{Left: 9223372036854775707, Right: 9223372036854775756},
			err:       nil,
		},
		{
			name:      "test6",
			rangeLeft: 50,
			rangeSize: 1000,
			expected:  &SequenceIdRange{Left: 50, Right: 1049},
			err:       nil,
		},
	} {
		actual, err := NewRangeBySize(tt.rangeLeft, tt.rangeSize)

		if tt.err == nil {
			assert.NoError(err, "test name: %s", tt.name)
			assert.Equal(tt.expected, actual, "test name %s", tt.name)
		} else {
			assert.Error(err, "test name: %s", tt.name)
		}
	}
}
