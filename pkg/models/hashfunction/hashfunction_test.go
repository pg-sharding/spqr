package hashfunction_test

import (
	"encoding/binary"
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/stretchr/testify/assert"
)

func EncodeUInt64Old(inp uint64) []byte {
	buf := make([]byte, 8)
	binary.PutUvarint(buf, inp)
	return buf
}

func TestEncodeUInt64_NewAndOldImplementations_AreTheSame(t *testing.T) {
	tests := []struct {
		name string
		inp  uint64
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
			resultNew := hashfunction.EncodeUInt64(tt.inp)
			resultOld := EncodeUInt64Old(tt.inp)
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
			result := hashfunction.EncodeUInt64(tt.inp)
			assert.Equal(t, tt.expected, result, "Test '%s': EncodeUInt64 should produce the expected result", tt.name)
		})
	}
}
