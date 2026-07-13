package planner

import (
	"context"
	"errors"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/virtual"
	"github.com/stretchr/testify/require"
)

func TestMetadataVirtualFunctionCallBlocksSessionAdvisoryLocksInTransactionPool(t *testing.T) {
	t.Parallel()

	sessionLockFunctions := []string{
		virtual.PGAdvisoryLock,
		virtual.PGAdvisoryUnlock,
		virtual.PgTryAdvisoryLock,
	}

	for _, fname := range sessionLockFunctions {
		t.Run(fname, func(t *testing.T) {
			t.Parallel()

			_, err := MetadataVirtualFunctionCall(
				context.Background(),
				advisoryLockRoutingMetadata(t, config.PoolModeTransaction),
				nil,
				fname,
				nil,
			)
			require.ErrorContains(t, err, "function execution is prohibited")

			var spqrErr *spqrerror.SpqrError
			require.True(t, errors.As(err, &spqrErr))
			require.Equal(t, spqrerror.SPQR_QUERY_BLOCKED, spqrErr.ErrorCode)
			require.Equal(t, "session-level advisory locks are unsafe in TRANSACTION pool mode", spqrErr.ErrDetail)
		})
	}
}

func TestMetadataVirtualFunctionCallAllowsTransactionAdvisoryLocksInTransactionPool(t *testing.T) {
	t.Parallel()

	p, err := MetadataVirtualFunctionCall(
		context.Background(),
		advisoryLockRoutingMetadata(t, config.PoolModeTransaction),
		nil,
		virtual.PGAdvisoryXactLock,
		nil,
	)

	require.NoError(t, err)
	require.IsType(t, &plan.ScatterPlan{}, p)
}

func TestMetadataVirtualFunctionCallAllowsSessionAdvisoryLocksInSessionPool(t *testing.T) {
	t.Parallel()

	sessionLockFunctions := []string{
		virtual.PGAdvisoryLock,
		virtual.PGAdvisoryUnlock,
		virtual.PgTryAdvisoryLock,
	}

	for _, fname := range sessionLockFunctions {
		t.Run(fname, func(t *testing.T) {
			t.Parallel()

			p, err := MetadataVirtualFunctionCall(
				context.Background(),
				advisoryLockRoutingMetadata(t, config.PoolModeSession),
				nil,
				fname,
				nil,
			)

			require.NoError(t, err)
			require.IsType(t, &plan.ScatterPlan{}, p)
		})
	}
}

func advisoryLockRoutingMetadata(t *testing.T, poolMode config.PoolMode) *rmeta.RoutingMetadataContext {
	t.Helper()

	sph := session.NewSimpleHandler("", false, "", "")
	sph.SetEnhancedMultiShardProcessing(session.VirtualParamLevelStatement, true)

	guc, err := sph.FindStrGUC(session.SPQR_ADVISORY_LOCK_BEHAVIOUR)
	require.NoError(t, err)
	guc.Set(sph, session.VirtualParamLevelStatement, string(config.AdvisoryLockBehaviourScatter))

	return &rmeta.RoutingMetadataContext{
		SPH: sph,
		ClientRule: &config.FrontendRule{
			PoolMode: poolMode,
		},
	}
}
