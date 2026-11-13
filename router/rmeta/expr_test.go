package rmeta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeduplicateRoutingValues(t *testing.T) {
	values := []any{"alpha", "beta", "alpha", int64(1), int64(1), "1"}

	assert.Equal(t, []any{"alpha", "beta", int64(1), "1"}, dedupRoutingValues(values))
}
