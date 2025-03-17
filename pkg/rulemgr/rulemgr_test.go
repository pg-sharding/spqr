package rulemgr

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/stretchr/testify/assert"
)

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
