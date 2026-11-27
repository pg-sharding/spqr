package plan_test

import (
	"encoding/json"
	"testing"

	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/planopts"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/xproto"
	"github.com/stretchr/testify/assert"
)

func TestShardDispatchPlanJSONMarshal(t *testing.T) {
	t.Parallel()

	_, err := json.Marshal(&plan.ShardDispatchPlan{})
	assert.NoError(t, err)
}

func TestCombineAutoLinearize(t *testing.T) {
	t.Parallel()

	p := plan.Combine(&plan.ScatterPlan{
		BasePlan: plan.BasePlan{H: planopts.PlanOpts{AutoLinearize: true}},
	}, &plan.ScatterPlan{})
	assert.True(t, p.Opts().AutoLinearize)
}

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

func TestParseResolveParamValueUUIDFormats(t *testing.T) {
	t.Parallel()

	binaryUUID := make([]byte, 16)
	binaryUUID[15] = 1

	for _, tc := range []struct {
		format int16
		value  []byte
	}{
		{xproto.FormatCodeText, []byte("00000000000000000000000000000001")},
		{xproto.FormatCodeBinary, binaryUUID},
	} {
		value, err := plan.ParseResolveParamValue(tc.format, 0, qdb.ColumnTypeUUID, [][]byte{tc.value})
		assert.NoError(t, err)
		assert.Equal(t, "00000000-0000-0000-0000-000000000001", value)
	}
}
