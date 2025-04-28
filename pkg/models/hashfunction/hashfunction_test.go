package hashfunction_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/stretchr/testify/assert"
)

func TestEncodeUInt64_NewAndOldImplementations_AreTheSame(t *testing.T) {
	tests := []struct {
		name string
		inp  uint64
	}{
		{"Zero value", 0},
		{"Power of two: 2^7", 128},
		{"Power of two: 2^10", 1024},
		{"Arbitrary number: 12345", 12345},
		{"Megabyte value", 1024 * 1024},
		{"Maximum 56-bit - 1 value", 1<<56 - 1},
		// this test panic {"Boundary value", 1<<56 - 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultNew := hashfunction.EncodeUInt64(tt.inp)
			resultOld := hashfunction.EncodeUInt64Old(tt.inp)
			assert.Equal(t, resultOld, resultNew, "EncodeUInt64 and EncodeUInt64Old should produce the same result")
		})
	}
}

func TestEncodeUInt64(t *testing.T) {
	tests := []struct {
		name     string
		inp      uint64
		expected []byte
	}{
		{"Zero value", 0, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{"Power of two: 2^7", 128, []byte{128, 1, 0, 0, 0, 0, 0, 0}},
		{"Power of two: 2^10", 1024, []byte{128, 8, 0, 0, 0, 0, 0, 0}},
		{"Arbitrary number: 12345", 12345, []byte{185, 96, 0, 0, 0, 0, 0, 0}},
		{"Megabyte value", 1024 * 1024, []byte{128, 128, 64, 0, 0, 0, 0, 0}},
		{"Maximum 56-bit - 1 value", 1<<56 - 1, []byte{255, 255, 255, 255, 255, 255, 255, 127}},
		{"Maximum 56-bit - 1 value", 1<<56, []byte{128, 128, 128, 128, 128, 128, 128, 128, 1, 0}},
		{"Large number: 2^60", 1 << 60, []byte{128, 128, 128, 128, 128, 128, 128, 128, 16, 0}},
		{"Large number: 2^63", 1 << 63, []byte{128, 128, 128, 128, 128, 128, 128, 128, 128, 1}},
		{"Large number: 2^64 - 1", (1 << 64) - 1, []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hashfunction.EncodeUInt64(tt.inp)
			assert.Equal(t, tt.expected, result, "Test '%s': EncodeUInt64 should produce the expected result", tt.name)
		})
	}
}
