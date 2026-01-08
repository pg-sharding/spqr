package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
)

func writeTempConfig(t *testing.T, contents string) string {
	t.Helper()

	dir := t.TempDir()
	p := filepath.Join(dir, "router.yaml")
	if err := os.WriteFile(p, []byte(contents), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return p
}

func minimalValidBaseYAML(extraQueryRouting string) string {
	return `
log_level: debug
router_mode: PROXY

frontend_rules:
  - db: db1
    usr: user1
    auth_rule:
      auth_method: ok

backend_rules: []
shards: {}

query_routing:
` + extraQueryRouting
}

func TestLoadRouterCfg_EngineDefaultV2_EnablesEnhancedMultiShardProcessing(t *testing.T) {
	yaml := minimalValidBaseYAML(`
  engine_default: v2`)

	path := writeTempConfig(t, yaml)

	_, err := config.LoadRouterCfg(path)
	if err != nil {
		t.Fatalf("LoadRouterCfg returned error: %v", err)
	}

	if got := config.RouterConfig().Qr.EnhancedMultiShardProcessing; got != true {
		t.Fatalf("EnhancedMultiShardProcessing = %v, want true (engine_default=v2)", got)
	}
}

func TestLoadRouterCfg_EngineDefaultEmpty_NoOp_DoesNotForceBool(t *testing.T) {
	// Set enhanced_multishard_processing to false and engine_default empty. Expect final to remain false.
	yaml := minimalValidBaseYAML(`
  enhanced_multishard_processing: false
  engine_default: ""
`)

	path := writeTempConfig(t, yaml)

	_, err := config.LoadRouterCfg(path)
	if err != nil {
		t.Fatalf("LoadRouterCfg returned error: %v", err)
	}

	if got := config.RouterConfig().Qr.EnhancedMultiShardProcessing; got != false {
		t.Fatalf("EnhancedMultiShardProcessing = %v, want false (engine_default empty is no-op)", got)
	}
}

func TestLoadRouterCfg_EngineDefaultInvalid_ReturnsError(t *testing.T) {
	yaml := minimalValidBaseYAML(`
  engine_default: v3
`)

	path := writeTempConfig(t, yaml)

	_, err := config.LoadRouterCfg(path)
	if err == nil {
		t.Fatalf("LoadRouterCfg expected error for invalid engine_default, got nil")
	}
	if !strings.Contains(err.Error(), "query_routing.engine_default") {
		t.Fatalf("error %q does not mention query_routing.engine_default", err.Error())
	}
}
