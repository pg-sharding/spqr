package route

import (
	"sync"
	"testing"

	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/stretchr/testify/assert"
)

func TestSetParams(t *testing.T) {
	t.Run("Set params and cachedParams", func(t *testing.T) {
		r := &Route{
			mu:           sync.Mutex{},
			cachedParams: false,
			params:       nil,
		}
		input := shard.ParameterSet{
			"host": "localhost",
			"port": "test",
		}
		r.SetParams(input)

		assert.True(t, r.cachedParams, "cachedParams must be true after setParams")
		assert.Equal(t, input, r.params, "params must match the input parameters")
	})

	t.Run("Overwrites old parameters", func(t *testing.T) {
		r := &Route{
			cachedParams: true,
			params:       shard.ParameterSet{"old": "value"},
		}
		newParams := shard.ParameterSet{"new": "value"}

		r.SetParams(newParams)

		assert.Equal(t, newParams, r.params, "parameters must be overwritten")
		assert.True(t, r.cachedParams, "cachedParams must remain true")
	})

	t.Run("Works with an empty parameter", func(t *testing.T) {
		r := &Route{}
		empty := shard.ParameterSet{}
		r.SetParams(empty)

		assert.True(t, r.cachedParams, "cachedParams must be true even for an empty parameter")
		assert.Empty(t, r.params, "params must be empty")
	})
}
