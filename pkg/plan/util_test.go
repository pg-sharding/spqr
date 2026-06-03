package plan_test

import (
	"testing"

	"github.com/google/uuid"
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

func TestParseResolveParamValueBinaryUUID(t *testing.T) {
	t.Parallel()

	u := uuid.MustParse("018f4b8e-37f0-7cc4-b5f2-0f62d09ca662")

	for _, colType := range []string{qdb.ColumnTypeUUID, qdb.ColumnTypeUUIDHashed} {
		val, err := plan.ParseResolveParamValue(xproto.FormatCodeBinary, 0, colType, [][]byte{u[:]})
		assert.NoError(t, err)
		assert.Equal(t, u.String(), val)
	}
}

func TestParseResolveParamValueBinaryUUIDInvalid(t *testing.T) {
	t.Parallel()

	_, err := plan.ParseResolveParamValue(xproto.FormatCodeBinary, 0, qdb.ColumnTypeUUIDHashed, [][]byte{[]byte("not-a-uuid")})
	assert.ErrorIs(t, err, plan.ErrResolvingValue)
}
