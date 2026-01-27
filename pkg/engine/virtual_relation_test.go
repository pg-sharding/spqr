package engine

import (
	"strings"
	"testing"

	"github.com/pg-sharding/spqr/qdb"
)

func TestCalculateCoverage(t *testing.T) {
	tests := []struct {
		name     string
		lower    interface{}
		upper    interface{}
		colType  string
		expected string
	}{
		// Integer type tests
		{
			name:     "Integer: small range in middle",
			lower:    int64(30),
			upper:    int64(40),
			colType:  qdb.ColumnTypeInteger,
			expected: "0.00%",
		},
		{
			name:     "Integer: range 1 to 11",
			lower:    int64(1),
			upper:    int64(11),
			colType:  qdb.ColumnTypeInteger,
			expected: "0.00%",
		},
		{
			name:     "Integer: range 10 to 20",
			lower:    int64(10),
			upper:    int64(20),
			colType:  qdb.ColumnTypeInteger,
			expected: "0.00%",
		},
		{
			name:     "Integer: zero-width range (upper <= lower)",
			lower:    int64(10),
			upper:    int64(10),
			colType:  qdb.ColumnTypeInteger,
			expected: "0.00%",
		},
		{
			name:     "Integer: inverse range (lower > upper)",
			lower:    int64(20),
			upper:    int64(10),
			colType:  qdb.ColumnTypeInteger,
			expected: "0.00%",
		},
		{
			name:     "Integer: large range in positive half",
			lower:    int64(0),
			upper:    int64(1000000000),
			colType:  qdb.ColumnTypeInteger,
			expected: "0.00%",
		},
		{
			name:     "Integer: large range in negative half",
			lower:    int64(-1000000000),
			upper:    int64(0),
			colType:  qdb.ColumnTypeInteger,
			expected: "0.00%",
		},
		{
			name:     "Integer: very large range (half of MaxInt64)",
			lower:    int64(0),
			upper:    int64(9223372036854775807 / 2),
			colType:  qdb.ColumnTypeInteger,
			expected: "25.00%",
		},
		// Unsigned integer tests
		{
			name:     "Uinteger: small range",
			lower:    uint64(1),
			upper:    uint64(11),
			colType:  qdb.ColumnTypeUinteger,
			expected: "0.00%",
		},
		{
			name:     "Uinteger: zero-width range",
			lower:    uint64(100),
			upper:    uint64(100),
			colType:  qdb.ColumnTypeUinteger,
			expected: "0.00%",
		},
		{
			name:     "Uinteger: large range",
			lower:    uint64(0),
			upper:    uint64(1000000000),
			colType:  qdb.ColumnTypeUinteger,
			expected: "0.00%",
		},
		{
			name:     "Uinteger: very large range (quarter of MaxUint64)",
			lower:    uint64(0),
			upper:    uint64(18446744073709551615 / 4),
			colType:  qdb.ColumnTypeUinteger,
			expected: "25.00%",
		},
		// Varchar hashed (same as uinteger)
		{
			name:     "VarcharHashed: small range",
			lower:    uint64(100),
			upper:    uint64(200),
			colType:  qdb.ColumnTypeVarcharHashed,
			expected: "0.00%",
		},
		// Unsupported types - should return N/A
		{
			name:     "Varchar: N/A",
			lower:    "abc",
			upper:    "xyz",
			colType:  qdb.ColumnTypeVarchar,
			expected: "N/A",
		},
		{
			name:     "UUID: medium range",
			lower:    "12345678-1234-1234-1234-123456789012",
			upper:    "87654321-4321-4321-4321-210987654321",
			colType:  qdb.ColumnTypeUUID,
			expected: "45.78%",
		},
		{
			name:     "UUID: sequential UUIDs",
			lower:    "00000000-0000-0000-0000-000000000000",
			upper:    "00000000-0000-0000-0000-000000000001",
			colType:  qdb.ColumnTypeUUID,
			expected: "0.00%",
		},
		{
			name:     "UUID: in half",
			lower:    "00000000-0000-0000-0000-000000000000",
			upper:    "80000000-0000-0000-0000-000000000000",
			colType:  qdb.ColumnTypeUUID,
			expected: "50.00%",
		},
		{
			name:     "UUID: almost in half",
			lower:    "00000000-0000-0000-0000-000000000000",
			upper:    "88888888-8888-8888-8888-888888888888",
			colType:  qdb.ColumnTypeUUID,
			expected: "53.33%",
		},
		{
			name:     "UUID: invalid format",
			lower:    "not-a-uuid",
			upper:    "also-not-a-uuid",
			colType:  qdb.ColumnTypeUUID,
			expected: "N/A",
		},
		{
			name:     "Unknown type: N/A",
			lower:    int64(10),
			upper:    int64(20),
			colType:  "unknown_type",
			expected: "N/A",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateCoverage(tt.lower, tt.upper, tt.colType)
			if result != tt.expected {
				t.Errorf("calculateCoverage(%v, %v, %q) = %q, expected %q",
					tt.lower, tt.upper, tt.colType, result, tt.expected)
			}
		})
	}
}

func TestCalculateCoverageEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		lower   interface{}
		upper   interface{}
		colType string
	}{
		{
			name:    "Integer: very close values",
			lower:   int64(100),
			upper:   int64(101),
			colType: qdb.ColumnTypeInteger,
		},
		{
			name:    "Integer: negative range",
			lower:   int64(-100),
			upper:   int64(-50),
			colType: qdb.ColumnTypeInteger,
		},
		{
			name:    "Uinteger: boundary values",
			lower:   uint64(0),
			upper:   uint64(1),
			colType: qdb.ColumnTypeUinteger,
		},
		{
			name:    "Uinteger: mid-range values",
			lower:   uint64(5000000000),
			upper:   uint64(5000000001),
			colType: qdb.ColumnTypeUinteger,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// These should not panic
			result := calculateCoverage(tt.lower, tt.upper, tt.colType)
			if result == "" {
				t.Errorf("calculateCoverage returned empty string")
			}
			// Result should be a percentage or N/A
			if result != "N/A" && !strings.Contains(result, "%") {
				t.Errorf("calculateCoverage returned invalid format: %q", result)
			}
		})
	}
}
