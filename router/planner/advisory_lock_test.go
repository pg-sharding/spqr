package planner

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/virtual"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdvisoryLockBoolAnd(t *testing.T) {
	sph := session.NewSimpleHandler("", false, "")
	sph.SetEnhancedMultiShardProcessing(session.VirtualParamLevelTxBlock, true)
	guc, err := sph.FindStrGUC(session.SPQR_ADVISORY_LOCK_BEHAVIOUR)
	require.NoError(t, err)
	require.NoError(t, guc.Set(sph, session.VirtualParamLevelTxBlock, "SCATTER"))
	p, err := MetadataVirtualFunctionCall(context.Background(), &rmeta.RoutingMetadataContext{SPH: sph},
		nil, virtual.PGTryAdvisoryXactLock, nil, nil)
	require.NoError(t, err)
	custom := p.(*plan.DataRowCustomProcess)
	process, prepare := custom.CustomRowProcess, custom.PrepareCustomRowProcess
	for _, values := range [][]byte{{'t', 'f'}, {1, 0}} {
		sh1, sh2 := kr.ShardKey{Name: "sh1"}, kr.ShardKey{Name: "sh2"}
		trueRow := &pgproto3.DataRow{Values: [][]byte{{values[0]}}}
		falseRow := &pgproto3.DataRow{Values: [][]byte{{values[1]}}}
		targets := []kr.ShardKey{sh1, sh2}

		custom.OpaqueInfo = prepare(targets)
		assert.Nil(t, process(falseRow, sh2, custom.OpaqueInfo))
		assert.Nil(t, process(falseRow, sh2, custom.OpaqueInfo))
		assert.Nil(t, process(trueRow, kr.ShardKey{Name: "other"}, custom.OpaqueInfo))
		assert.Equal(t, &pgproto3.DataRow{Values: [][]byte{{values[1]}}}, process(trueRow, sh1, custom.OpaqueInfo))

		// A new execution must discard an incomplete previous result.
		custom.OpaqueInfo = prepare(targets)
		assert.Nil(t, process(falseRow, sh1, custom.OpaqueInfo))
		custom.OpaqueInfo = prepare(targets)
		trueRow = &pgproto3.DataRow{Values: [][]byte{{values[0]}}}
		assert.Nil(t, process(trueRow, sh1, custom.OpaqueInfo))
		assert.Equal(t, &pgproto3.DataRow{Values: [][]byte{{values[0]}}}, process(trueRow, sh2, custom.OpaqueInfo))

		custom.OpaqueInfo = prepare(targets)
		assert.Nil(t, process(trueRow, sh1, custom.OpaqueInfo))
		assert.Equal(t, &pgproto3.DataRow{Values: [][]byte{nil}},
			process(&pgproto3.DataRow{Values: [][]byte{nil}}, sh2, custom.OpaqueInfo))
	}
}
