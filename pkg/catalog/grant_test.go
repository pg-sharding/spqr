package catalog

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
)

func TestCheckGrants(t *testing.T) {
	tests := []struct {
		name          string
		target        string
		actual        []config.Role
		enableRoleSys bool
		wantErr       bool
	}{
		{
			name:          "Role system disabled",
			target:        RoleWriter,
			actual:        []config.Role{RoleReader},
			enableRoleSys: false,
			wantErr:       false,
		},
		{
			name:          "Role system enabled, has target role",
			target:        RoleWriter,
			actual:        []config.Role{RoleWriter},
			enableRoleSys: true,
			wantErr:       false,
		},
		{
			name:          "Role system enabled, has admin role",
			target:        RoleWriter,
			actual:        []config.Role{RoleAdmin},
			enableRoleSys: true,
			wantErr:       false,
		},
		{
			name:          "Role system enabled, does not have target role",
			target:        RoleWriter,
			actual:        []config.Role{RoleReader},
			enableRoleSys: true,
			wantErr:       true,
		},
		{
			name:          "Role system enabled, does not have admin role",
			target:        RoleAdmin,
			actual:        []config.Role{RoleReader, RoleWriter},
			enableRoleSys: true,
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.RouterConfig().EnableRoleSystem = tt.enableRoleSys
			err := CheckGrants(tt.target, &config.FrontendRule{Grants: tt.actual})
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckGrants() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
