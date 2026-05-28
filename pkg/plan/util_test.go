package plan_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/xproto"
	"github.com/stretchr/testify/assert"
)

func TestParseResolveParamValueEmptyBindParams(t *testing.T) {
	t.Parallel()

	_, err := plan.ParseResolveParamValue(xproto.FormatCodeText, 0, qdb.ColumnTypeInteger, [][]byte{})
	assert.ErrorIs(t, err, plan.ErrResolvingValue)
}

func TestParseResolveParamValueOutOfRangeIndex(t *testing.T) {
	t.Parallel()

	_, err := plan.ParseResolveParamValue(xproto.FormatCodeText, 1, qdb.ColumnTypeInteger, [][]byte{[]byte("1")})
	assert.ErrorIs(t, err, plan.ErrResolvingValue)
}

func TestParseResolveParamValueNegativeIndex(t *testing.T) {
	t.Parallel()

	_, err := plan.ParseResolveParamValue(xproto.FormatCodeText, -1, qdb.ColumnTypeInteger, [][]byte{[]byte("1")})
	assert.ErrorIs(t, err, plan.ErrResolvingValue)
}

func TestParseResolveParamValueValidBind(t *testing.T) {
	t.Parallel()

	val, err := plan.ParseResolveParamValue(xproto.FormatCodeText, 0, qdb.ColumnTypeInteger, [][]byte{[]byte("42")})
	assert.NoError(t, err)
	assert.Equal(t, int64(42), val)
}
