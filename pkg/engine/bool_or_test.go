package engine_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/engine"
	"github.com/stretchr/testify/assert"
)

func TestBoolOr(t *testing.T) {
	for _, values := range [][]byte{{'t', 'f'}, {1, 0}} {
		trueValue, falseValue := values[:1], values[1:]
		assert.Equal(t, trueValue, engine.BoolOr(trueValue, trueValue))
		assert.Equal(t, trueValue, engine.BoolOr(trueValue, falseValue))
		assert.Equal(t, trueValue, engine.BoolOr(falseValue, trueValue))
		assert.Equal(t, falseValue, engine.BoolOr(falseValue, falseValue))
		assert.Equal(t, trueValue, engine.BoolOr(trueValue, nil))
		assert.Equal(t, trueValue, engine.BoolOr(nil, trueValue))
		assert.Nil(t, engine.BoolOr(falseValue, nil))
		assert.Nil(t, engine.BoolOr(nil, falseValue))
		assert.Nil(t, engine.BoolOr(nil, nil))
	}
}
