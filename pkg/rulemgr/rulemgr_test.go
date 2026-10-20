package rulemgr

import (
	"testing"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultRulesKeepSettingsAtStartupAndReload(t *testing.T) {
	frontend := &config.FrontendRule{
		PoolDefault: true, SearchPath: "tenant", PoolMode: config.PoolModeTransaction,
		AuthRule: &config.AuthCfg{Method: config.AuthOK}, PoolDiscard: true,
		PoolRollback: true, PoolPreparedStatement: true,
	}
	backend := &config.BackendRule{
		PoolDefault: true, DefaultAuthRule: &config.AuthBackendCfg{Password: "secret"},
		AuthRules:       map[string]*config.AuthBackendCfg{"shard": {Usr: "backend-user"}},
		ConnectionLimit: 7, ConnectionRetries: 3, ConnectionTimeout: 4 * time.Second,
		ServerLifetime: time.Minute, KeepAlive: 5 * time.Second, TCPUserTimeout: 6 * time.Second,
		DisableJitter: true,
	}
	key := *route.NewRouteKey("tenant-user", "tenant-db")
	mgr := NewMgr([]*config.FrontendRule{frontend}, []*config.BackendRule{backend})
	for _, phase := range []string{"startup", "reload"} {
		t.Run(phase, func(t *testing.T) {
			if phase == "reload" {
				mgr.Reload([]*config.FrontendRule{frontend}, []*config.BackendRule{backend})
			}
			fr, err := mgr.MatchKeyFrontend(key)
			require.NoError(t, err)
			wantFrontend := *frontend
			wantFrontend.Usr, wantFrontend.DB = key.Usr(), key.DB()
			require.Equal(t, wantFrontend, *fr)
			be, err := mgr.MatchKeyBackend(key)
			require.NoError(t, err)
			wantBackend := *backend
			wantBackend.Usr, wantBackend.DB = key.Usr(), key.DB()
			require.Equal(t, wantBackend, *be)
			require.Empty(t, frontend.Usr)
			require.Empty(t, backend.DB)
		})
	}
}

func TestParseRules(t *testing.T) {
	tests := []struct {
		name              string
		frontendRules     []*config.FrontendRule
		backendRules      []*config.BackendRule
		wantFrontendRules map[route.Key]*config.FrontendRule
		wantBackendRules  map[route.Key]*config.BackendRule
		wantDefaultFR     *config.FrontendRule
		wantDefaultBR     *config.BackendRule
	}{
		{
			name:              "empty rules",
			frontendRules:     []*config.FrontendRule{},
			backendRules:      []*config.BackendRule{},
			wantFrontendRules: map[route.Key]*config.FrontendRule{},
			wantBackendRules:  map[route.Key]*config.BackendRule{},
			wantDefaultFR:     nil,
			wantDefaultBR:     nil,
		},
		{
			name:              "nil rules",
			frontendRules:     nil,
			backendRules:      nil,
			wantFrontendRules: map[route.Key]*config.FrontendRule{},
			wantBackendRules:  map[route.Key]*config.BackendRule{},
			wantDefaultFR:     nil,
			wantDefaultBR:     nil,
		},
		{
			name: "single rules with defaults",
			frontendRules: []*config.FrontendRule{
				{
					Usr:         "user1",
					DB:          "db1",
					PoolDefault: false,
				},
				{
					Usr:         "default",
					DB:          "default",
					PoolDefault: true,
				},
			},
			backendRules: []*config.BackendRule{
				{
					Usr:         "user1",
					DB:          "db1",
					PoolDefault: false,
				},
				{
					Usr:         "default",
					DB:          "default",
					PoolDefault: true,
				},
			},
			wantFrontendRules: map[route.Key]*config.FrontendRule{
				*route.NewRouteKey("user1", "db1"): {
					Usr:         "user1",
					DB:          "db1",
					PoolDefault: false,
				},
			},
			wantBackendRules: map[route.Key]*config.BackendRule{
				*route.NewRouteKey("user1", "db1"): {
					Usr:         "user1",
					DB:          "db1",
					PoolDefault: false,
				},
			},
			wantDefaultFR: &config.FrontendRule{
				Usr:         "default",
				DB:          "default",
				PoolDefault: true,
			},
			wantDefaultBR: &config.BackendRule{
				Usr:         "default",
				DB:          "default",
				PoolDefault: true,
			},
		},
		{
			name: "only frontend rules exist",
			frontendRules: []*config.FrontendRule{
				{
					Usr:         "user1",
					DB:          "db1",
					PoolDefault: false,
				},
				{
					Usr:         "user2",
					DB:          "db2",
					PoolDefault: false,
				},
				{
					Usr:         "default",
					DB:          "default",
					PoolDefault: true,
				},
			},
			backendRules: []*config.BackendRule{},
			wantFrontendRules: map[route.Key]*config.FrontendRule{
				*route.NewRouteKey("user1", "db1"): {
					Usr:         "user1",
					DB:          "db1",
					PoolDefault: false,
				},
				*route.NewRouteKey("user2", "db2"): {
					Usr:         "user2",
					DB:          "db2",
					PoolDefault: false,
				},
			},
			wantBackendRules: map[route.Key]*config.BackendRule{},
			wantDefaultFR: &config.FrontendRule{
				Usr:         "default",
				DB:          "default",
				PoolDefault: true,
			},
			wantDefaultBR: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFR, gotBR, gotDefaultFR, gotDefaultBR := parseRules(tt.frontendRules, tt.backendRules)

			assert.Equal(t, tt.wantFrontendRules, gotFR)
			assert.Equal(t, tt.wantBackendRules, gotBR)
			assert.Equal(t, tt.wantDefaultFR, gotDefaultFR)
			assert.Equal(t, tt.wantDefaultBR, gotDefaultBR)
		})
	}
}
