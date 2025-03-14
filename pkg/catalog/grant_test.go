package catalog

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
)

func TestCheckGrants(t *testing.T) {
	tests := []struct {
		name                    string
		target                  string
		rule                    *config.FrontendRule
		enableRoleSys           bool
		tableGroups             []config.TableGroup
		wantNewGrantsCheckerErr bool
		wantCheckGrantsErr      bool
	}{
		{
			name:                    "Role system disabled",
			target:                  RoleWriter,
			rule:                    &config.FrontendRule{Usr: "user1", DB: "db1"},
			enableRoleSys:           false,
			wantNewGrantsCheckerErr: false,
			wantCheckGrantsErr:      false,
		},
		{
			name:                    "Role system enabled, has target role",
			target:                  RoleWriter,
			rule:                    &config.FrontendRule{Usr: "user1", DB: "db1"},
			enableRoleSys:           true,
			tableGroups:             []config.TableGroup{{Writers: []string{"user1"}}},
			wantNewGrantsCheckerErr: false,
			wantCheckGrantsErr:      false,
		},
		{
			name:                    "Role system enabled, does not have target role",
			target:                  RoleWriter,
			rule:                    &config.FrontendRule{Usr: "user2", DB: "db1"},
			enableRoleSys:           true,
			tableGroups:             []config.TableGroup{{Writers: []string{"user1"}}},
			wantNewGrantsCheckerErr: false,
			wantCheckGrantsErr:      true,
		},
		{
			name:                    "Role system enabled, has admin role",
			target:                  RoleAdmin,
			rule:                    &config.FrontendRule{Usr: "admin1", DB: "db1"},
			enableRoleSys:           true,
			tableGroups:             []config.TableGroup{{Admins: []string{"admin1"}}},
			wantNewGrantsCheckerErr: false,
			wantCheckGrantsErr:      false,
		},
		{
			name:                    "Role system enabled, has admin role but reader role requested",
			target:                  RoleReader,
			rule:                    &config.FrontendRule{Usr: "admin1", DB: "db1"},
			enableRoleSys:           true,
			tableGroups:             []config.TableGroup{{Admins: []string{"admin1"}}},
			wantNewGrantsCheckerErr: false,
			wantCheckGrantsErr:      false,
		},
		{
			name:                    "Role system enabled, unknown role",
			target:                  "unknown",
			rule:                    &config.FrontendRule{Usr: "user1", DB: "db1"},
			enableRoleSys:           true,
			tableGroups:             []config.TableGroup{{Writers: []string{"user1"}}},
			wantNewGrantsCheckerErr: false,
			wantCheckGrantsErr:      true,
		},
		{
			name:                    "Multiple table groups not supported",
			target:                  RoleWriter,
			rule:                    &config.FrontendRule{Usr: "user1", DB: "db1"},
			enableRoleSys:           true,
			tableGroups:             []config.TableGroup{{Writers: []string{"user1"}}, {Writers: []string{"user2"}}},
			wantNewGrantsCheckerErr: true,
			wantCheckGrantsErr:      false,
		},
		{
			name:                    "zero table groups is not possible when role system is enabled",
			target:                  RoleWriter,
			rule:                    &config.FrontendRule{Usr: "user1", DB: "db1"},
			enableRoleSys:           true,
			tableGroups:             []config.TableGroup{},
			wantNewGrantsCheckerErr: true,
			wantCheckGrantsErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker, err := NewGrantsChecker(tt.enableRoleSys, tt.tableGroups)
			if err != nil {
				if (err != nil) != tt.wantNewGrantsCheckerErr {
					t.Errorf("NewGrantsChecker() error = %v, wantErr %v", err, tt.wantNewGrantsCheckerErr)
				}
				return
			}

			if checker == nil {
				t.Errorf("NewGrantsChecker() returned nil")
				return
			}
			err = checker.CheckGrants(tt.target, tt.rule)
			if (err != nil) != tt.wantCheckGrantsErr {
				t.Errorf("CheckGrants() error = %v, wantErr %v", err, tt.wantCheckGrantsErr)
			}
		})
	}
}
