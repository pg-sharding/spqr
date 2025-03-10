package catalog

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
)

func TestCheckGrants(t *testing.T) {
	tests := []struct {
		name          string
		target        string
		rule          *config.FrontendRule
		enableRoleSys bool
		tableGroups   []config.TableGroup
		wantErr       bool
	}{
		{
			name:          "Role system disabled",
			target:        RoleWriter,
			rule:          &config.FrontendRule{Usr: "user1", DB: "db1"},
			enableRoleSys: false,
			wantErr:       false,
		},
		{
			name:          "Role system enabled, has target role",
			target:        RoleWriter,
			rule:          &config.FrontendRule{Usr: "user1", DB: "db1"},
			enableRoleSys: true,
			tableGroups:   []config.TableGroup{{Writers: []string{"user1"}}},
			wantErr:       false,
		},
		{
			name:          "Role system enabled, does not have target role",
			target:        RoleWriter,
			rule:          &config.FrontendRule{Usr: "user2", DB: "db1"},
			enableRoleSys: true,
			tableGroups:   []config.TableGroup{{Writers: []string{"user1"}}},
			wantErr:       true,
		},
		{
			name:          "Role system enabled, has admin role",
			target:        RoleAdmin,
			rule:          &config.FrontendRule{Usr: "admin1", DB: "db1"},
			enableRoleSys: true,
			tableGroups:   []config.TableGroup{{Admins: []string{"admin1"}}},
			wantErr:       false,
		},
		{
			name:          "Role system enabled, has admin role but reader role requested",
			target:        RoleReader,
			rule:          &config.FrontendRule{Usr: "admin1", DB: "db1"},
			enableRoleSys: true,
			tableGroups:   []config.TableGroup{{Admins: []string{"admin1"}}},
			wantErr:       false,
		},
		{
			name:          "Role system enabled, unknown role",
			target:        "unknown",
			rule:          &config.FrontendRule{Usr: "user1", DB: "db1"},
			enableRoleSys: true,
			tableGroups:   []config.TableGroup{{Writers: []string{"user1"}}},
			wantErr:       true,
		},
		{
			name:          "Multiple table groups not supported",
			target:        RoleWriter,
			rule:          &config.FrontendRule{Usr: "user1", DB: "db1"},
			enableRoleSys: true,
			tableGroups:   []config.TableGroup{{Writers: []string{"user1"}}, {Writers: []string{"user2"}}},
			wantErr:       true,
		},
		{
			name:          "zero table groups is not possible when role system is enabled",
			target:        RoleWriter,
			rule:          &config.FrontendRule{Usr: "user1", DB: "db1"},
			enableRoleSys: true,
			tableGroups:   []config.TableGroup{},
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.RouterConfig().EnableRoleSystem = tt.enableRoleSys
			config.RolesConfig().TableGroups = tt.tableGroups
			err := CheckGrants(tt.target, tt.rule)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckGrants() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
