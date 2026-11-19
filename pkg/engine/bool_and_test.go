package engine_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/engine"
	"github.com/stretchr/testify/assert"
)

func TestBoolAnd(t *testing.T) {
	for _, values := range [][]byte{{'t', 'f'}, {1, 0}} {
		trueValue, falseValue := values[:1], values[1:]
		assert.Equal(t, trueValue, engine.BoolAnd(trueValue, trueValue))
		assert.Equal(t, falseValue, engine.BoolAnd(trueValue, falseValue))
		assert.Equal(t, falseValue, engine.BoolAnd(falseValue, trueValue))
		assert.Equal(t, falseValue, engine.BoolAnd(falseValue, falseValue))
		assert.Equal(t, falseValue, engine.BoolAnd(falseValue, nil))
		assert.Equal(t, falseValue, engine.BoolAnd(nil, falseValue))
		assert.Nil(t, engine.BoolAnd(trueValue, nil))
		assert.Nil(t, engine.BoolAnd(nil, trueValue))
		assert.Nil(t, engine.BoolAnd(nil, nil))
	}
}
