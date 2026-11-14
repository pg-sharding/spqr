package session_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/stretchr/testify/require"
)

func TestSessionPinRequiresForcedCleanupDisabled(t *testing.T) {
	cfg := config.RouterConfig()
	old := cfg.ForceConnectionCleanup
	t.Cleanup(func() { cfg.ForceConnectionCleanup = old })

	sph := session.NewSimpleHandler("", false, "")
	guc, err := sph.FindBoolGUC(session.SPQR_SESSION_CONNECTIONS_PIN)
	require.NoError(t, err)

	cfg.ForceConnectionCleanup = true
	require.NoError(t, guc.Set(sph, session.VirtualParamLevelTxBlock, false))
	err = guc.Set(sph, session.VirtualParamLevelTxBlock, true)
	require.ErrorContains(t, err, "force_connection_cleanup")
	require.False(t, guc.Get(sph))

	cfg.ForceConnectionCleanup = false
	require.NoError(t, guc.Set(sph, session.VirtualParamLevelTxBlock, true))
	require.True(t, guc.Get(sph))
}
