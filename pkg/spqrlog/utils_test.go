package spqrlog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkPrintfAddress(b *testing.B) {
    num := 10
    for i := 0; i < b.N; i++ {
        _ = fmt.Sprintf("%p", &num)
    }
}

func BenchmarkGetPointer(b *testing.B) {
    num := 10
    for i := 0; i < b.N; i++ {
        _ = GetPointer(&num)
    }
}

type Denis struct {
	Age int
}

func TestGetPointer(t *testing.T) {

	tests := []interface{}{true, 123,"denis", Denis{Age:25}}
	for _, test := range tests {
		expected := fmt.Sprintf("%p", &test)
	
		result := GetPointer(&test)
		fmtOutput := fmt.Sprintf("0x%x", result)
		
		assert.Equal(t, expected, fmtOutput)
	}
}

