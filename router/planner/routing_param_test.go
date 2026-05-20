package planner

import (
	"testing"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/xproto"
	"github.com/stretchr/testify/assert"
)

func TestCalculateRoutingListTupleItemValueEmptyBindParams(t *testing.T) {
	t.Parallel()

	sph := session.NewSimpleHandler("", false, "", "")
	rm := &rmeta.RoutingMetadataContext{SPH: sph}

	_, err := CalculateRoutingListTupleItemValue(
		rm,
		qdb.ColumnTypeInteger,
		&lyx.ParamRef{Number: 1},
		[]int16{0, 0},
	)
	assert.ErrorIs(t, err, plan.ErrResolvingValue)
}

func TestCalculateRoutingListTupleItemValueEmptyFormatCodes(t *testing.T) {
	t.Parallel()

	sph := session.NewSimpleHandler("", false, "", "")
	rm := &rmeta.RoutingMetadataContext{SPH: sph}

	_, err := CalculateRoutingListTupleItemValue(
		rm,
		qdb.ColumnTypeInteger,
		&lyx.ParamRef{Number: 1},
		[]int16{},
	)
	assert.ErrorIs(t, err, plan.ErrResolvingValue)
}

func TestCalculateRoutingListTupleItemValueInvalidParamNumber(t *testing.T) {
	t.Parallel()

	sph := session.NewSimpleHandler("", false, "", "")
	rm := &rmeta.RoutingMetadataContext{SPH: sph}

	_, err := CalculateRoutingListTupleItemValue(
		rm,
		qdb.ColumnTypeInteger,
		&lyx.ParamRef{Number: 0},
		[]int16{xproto.FormatCodeText},
	)
	assert.ErrorIs(t, err, plan.ErrResolvingValue)
}

func TestCalculateRoutingListTupleItemValueValidBind(t *testing.T) {
	t.Parallel()

	sph := session.NewSimpleHandler("", false, "", "")
	sph.SetBindParams([][]byte{[]byte("42")})
	sph.SetParamFormatCodes([]int16{xproto.FormatCodeText})

	rm := &rmeta.RoutingMetadataContext{SPH: sph}

	val, err := CalculateRoutingListTupleItemValue(
		rm,
		qdb.ColumnTypeInteger,
		&lyx.ParamRef{Number: 1},
		[]int16{xproto.FormatCodeText},
	)
	assert.NoError(t, err)
	assert.Equal(t, int64(42), val)
}
