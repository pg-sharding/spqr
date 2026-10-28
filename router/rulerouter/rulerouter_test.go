package rulerouter_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/rulerouter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuleRouterReloadShowNoticeMessages(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "router.yaml")

	initialCfg := `
log_level: info
router_port: "6432"
admin_console_port: "7432"
show_notice_messages: false
frontend_rules:
  - db: test
    usr: test
    auth_rule:
      auth_method: ok
shards:
  sh1:
    type: DATA
    hosts:
      - "localhost:6432"
`
	err := os.WriteFile(cfgPath, []byte(initialCfg), 0o600)
	require.NoError(t, err)

	_, err = config.LoadRouterCfg(cfgPath)
	require.NoError(t, err)
	rcfg := config.RouterConfig()
	assert.False(t, rcfg.ShowNoticeMessages, "expected initial show_notice_messages to be false")

	router := rulerouter.NewRouter(nil, nil, rcfg, nil)
	require.NotNil(t, router)

	// Update config to enable show_notice_messages: true
	updatedCfg := `
log_level: info
router_port: "6432"
admin_console_port: "7432"
show_notice_messages: true
frontend_rules:
  - db: test
    usr: test
    auth_rule:
      auth_method: ok
shards:
  sh1:
    type: DATA
    hosts:
      - "localhost:6432"
`
	err = os.WriteFile(cfgPath, []byte(updatedCfg), 0o600)
	require.NoError(t, err)

	// Trigger SIGHUP reload path
	err = router.Reload(cfgPath)
	require.NoError(t, err)

	reloadedCfg := config.RouterConfig()
	assert.True(t, reloadedCfg.ShowNoticeMessages, "expected show_notice_messages to be reloaded to true")

	// Now update config back to false and verify reload again
	err = os.WriteFile(cfgPath, []byte(initialCfg), 0o600)
	require.NoError(t, err)

	err = router.Reload(cfgPath)
	require.NoError(t, err)

	reloadedCfg2 := config.RouterConfig()
	assert.False(t, reloadedCfg2.ShowNoticeMessages, "expected show_notice_messages to be reloaded to false")
}
