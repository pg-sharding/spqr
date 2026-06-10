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

func TestParseResolveParamValueBinaryUUIDRawBytes(t *testing.T) {
	t.Parallel()

	rawUUID := []byte{
		0x00, 0x01, 0x02, 0x03,
		0x04, 0x05,
		0x06, 0x07,
		0x08, 0x09,
		0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	}

	tests := []struct {
		name       string
		columnType string
	}{
		{
			name:       "UUID",
			columnType: qdb.ColumnTypeUUID,
		},
		{
			name:       "UUIDHashed",
			columnType: qdb.ColumnTypeUUIDHashed,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			val, err := plan.ParseResolveParamValue(xproto.FormatCodeBinary, 0, tt.columnType, [][]byte{rawUUID})
			assert.NoError(t, err)
			assert.Equal(t, "00010203-0405-0607-0809-0a0b0c0d0e0f", val)
		})
	}
}

func TestParseResolveParamValueBinaryUUIDTextFallback(t *testing.T) {
	t.Parallel()

	uuidText := "550e8400-e29b-41d4-a716-446655440000"

	val, err := plan.ParseResolveParamValue(xproto.FormatCodeBinary, 0, qdb.ColumnTypeUUID, [][]byte{[]byte(uuidText)})
	assert.NoError(t, err)
	assert.Equal(t, uuidText, val)
}

func TestParseResolveParamValueTextUUID(t *testing.T) {
	t.Parallel()

	uuidText := "550e8400-e29b-41d4-a716-446655440000"

	val, err := plan.ParseResolveParamValue(xproto.FormatCodeText, 0, qdb.ColumnTypeUUID, [][]byte{[]byte(uuidText)})
	assert.NoError(t, err)
	assert.Equal(t, uuidText, val)
}

func TestParseResolveParamValueValidBind(t *testing.T) {
	t.Parallel()

	val, err := plan.ParseResolveParamValue(xproto.FormatCodeText, 0, qdb.ColumnTypeInteger, [][]byte{[]byte("42")})
	assert.NoError(t, err)
	assert.Equal(t, int64(42), val)
}
