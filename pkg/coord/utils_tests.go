package coord_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/coord"
)

func TestRandomHex(t *testing.T) {
	t.Run("Success case", func(t *testing.T) {
		output, err := coord.RandomHex(16)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if len(output) != 32 {
			t.Errorf("Expected string of length 32, got %v", len(output))
		}
	})
}
