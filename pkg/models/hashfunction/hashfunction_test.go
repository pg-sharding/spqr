package hashfunction_test

import (
	"encoding/binary"
	"testing"

	"github.com/go-faster/city"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"
)

func EncodeUInt64Old(input uint64) []byte {
	buf := make([]byte, 8)
	binary.PutUvarint(buf, input)
	return buf
}

func TestEncodeUInt64_NewAndOldImplementations_AreTheSame(t *testing.T) {
	tests := []struct {
		name  string
		input uint64
	}{
		{"zero", 0},
		{"128", 128},
		{"1024", 1024},
		{"12345", 12345},
		{"1024 * 1024", 1024 * 1024},
		{"1<<56 - 1", 1<<56 - 1},
		// this test panic {"1<<56", 1<<56},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultNew := hashfunction.EncodeUInt64(tt.input)
			resultOld := EncodeUInt64Old(tt.input)
			assert.Equal(t, resultOld, resultNew, "EncodeUInt64 and EncodeUInt64Old should produce the same result")
		})
	}
}

func TestEncodeUInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected []byte
	}{
		{"Zero value", 0, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{"128", 128, []byte{128, 1, 0, 0, 0, 0, 0, 0}},
		{"1024", 1024, []byte{128, 8, 0, 0, 0, 0, 0, 0}},
		{"12345", 12345, []byte{185, 96, 0, 0, 0, 0, 0, 0}},
		{"1024 * 1024", 1024 * 1024, []byte{128, 128, 64, 0, 0, 0, 0, 0}},
		{"1<<56 - 1", 1<<56 - 1, []byte{255, 255, 255, 255, 255, 255, 255, 127}},
		{"1<<56", 1 << 56, []byte{128, 128, 128, 128, 128, 128, 128, 128, 1, 0}},
		{"1<<56 + 1", 1<<56 + 1, []byte{129, 128, 128, 128, 128, 128, 128, 128, 1, 0}},
		{"1 << 60", 1 << 60, []byte{128, 128, 128, 128, 128, 128, 128, 128, 16, 0}},
		{"1 << 63", 1 << 63, []byte{128, 128, 128, 128, 128, 128, 128, 128, 128, 1}},
		{"(1 << 64) - 1", (1 << 64) - 1, []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 1}},
		// this test panic {"1<<64", 1<<64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hashfunction.EncodeUInt64(tt.input)
			assert.Equal(t, tt.expected, result, "Test '%s': EncodeUInt64 should produce the expected result", tt.name)
		})
	}
}

func TestApplyHashFunction(t *testing.T) {
	hfMur, _ := hashfunction.HashFunctionByName("murmur")
	hfCity, _ := hashfunction.HashFunctionByName("city")
	assert := assert.New(t)

	type tcase struct {
		input        int64
		expected     uint64
		hashfunction hashfunction.HashFunctionType
		columnType   string
		err          error
	}

	for _, tt := range []tcase{
		{
			input:        9223372036854775807,
			expected:     3119878097,
			hashfunction: hfMur,
			columnType:   qdb.ColumnTypeInteger,
			err:          nil,
		},
		{
			input:        9223372036854775807,
			expected:     2380995212,
			hashfunction: hfCity,
			columnType:   qdb.ColumnTypeInteger,
			err:          nil,
		},
		{
			input:        -9223372036854775808,
			expected:     1367477491,
			hashfunction: hfMur,
			columnType:   qdb.ColumnTypeInteger,
			err:          nil,
		},
		{
			input:        -9223372036854775808,
			expected:     1856670072,
			hashfunction: hfCity,
			columnType:   qdb.ColumnTypeInteger,
			err:          nil,
		},
	} {
		result, err := hashfunction.ApplyHashFunction(tt.input, tt.columnType, tt.hashfunction)

		if tt.err == nil {
			assert.NoError(err, "query %s", tt.input)
		} else {
			assert.Error(err, "query %s", tt.input)
		}

		assert.Equal(tt.expected, result, "query %s", tt.input)
	}
}

func TestApplyHashFunction_UUIDHashed(t *testing.T) {
	hfMur, _ := hashfunction.HashFunctionByName("murmur")
	hfCity, _ := hashfunction.HashFunctionByName("city")
	input := "018f4b8e-37f0-7cc4-b5f2-0f62d09ca662"
	upperInput := "018F4B8E-37F0-7CC4-B5F2-0F62D09CA662"

	murmurResult, err := hashfunction.ApplyHashFunction(upperInput, qdb.ColumnTypeUUIDHashed, hfMur)
	assert.NoError(t, err)
	assert.Equal(t, uint64(murmur3.Sum32([]byte(input))), murmurResult)

	cityResult, err := hashfunction.ApplyHashFunction([]byte(input), qdb.ColumnTypeUUIDHashed, hfCity)
	assert.NoError(t, err)
	assert.Equal(t, uint64(city.Hash32([]byte(input))), cityResult)

	_, err = hashfunction.ApplyHashFunction("not-a-uuid", qdb.ColumnTypeUUIDHashed, hfMur)
	assert.Error(t, err)
}

func TestApplyMurmurHashNegative(t *testing.T) {
	assert := assert.New(t)
	uintVal := uint64(0)
	_, err := hashfunction.ApplyMurmurHashFunction(uintVal, qdb.ColumnTypeInteger)
	assert.Error(err, "incorrect int")
	intVal := int64(0)
	_, err = hashfunction.ApplyMurmurHashFunction(intVal, qdb.ColumnTypeUinteger)
	assert.Error(err, "incorrect uint")
}

func TestApplyCityHashNegative(t *testing.T) {
	assert := assert.New(t)
	uintVal := uint64(0)
	_, err := hashfunction.ApplyCityHashFunction(uintVal, qdb.ColumnTypeInteger)
	assert.Error(err, "incorrect int")
	intVal := int64(0)
	_, err = hashfunction.ApplyCityHashFunction(intVal, qdb.ColumnTypeUinteger)
	assert.Error(err, "incorrect uint")
}
