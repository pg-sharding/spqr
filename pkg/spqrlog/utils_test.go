package spqrlog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// BenchmarkPrintfAddress benchmarks the performance of the fmt.Sprintf function
// when formatting the address of a variable.
func BenchmarkPrintfAddress(b *testing.B) {
	num := 10
	for range b.N {
		_ = fmt.Sprintf("%p", &num)
	}
}

// BenchmarkGetPointer is a benchmark function that measures the performance of the GetPointer function.
// It repeatedly calls the GetPointer function with a pointer to an integer and records the execution time.
// The benchmark is run by the testing framework.
func BenchmarkGetPointer(b *testing.B) {
	num := 10
	for range b.N {
		_ = GetPointer(&num)
	}
}

type Denis struct {
	Age int
}

// TestGetPointer tests the GetPointer function.
// It verifies that the function returns the correct memory address of the input value.
func TestGetPointer(t *testing.T) {

	tests := []any{true, 123, "denis", Denis{Age: 25}}
	for _, test := range tests {
		expected := fmt.Sprintf("%p", &test)

		result := GetPointer(&test)
		fmtOutput := fmt.Sprintf("0x%x", result)

		assert.Equal(t, expected, fmtOutput)
	}
}
