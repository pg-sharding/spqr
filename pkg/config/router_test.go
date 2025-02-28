package config

import (
	"testing"
)

func TestCheckGrants(t *testing.T) {
	tests := []struct {
		name          string
		target        Role
		actual        []Role
		enableRoleSys bool
		wantErr       bool
	}{
		{
			name:          "Role system disabled",
			target:        RoleWriter,
			actual:        []Role{RoleReader},
			enableRoleSys: false,
			wantErr:       false,
		},
		{
			name:          "Role system enabled, has target role",
			target:        RoleWriter,
			actual:        []Role{RoleWriter},
			enableRoleSys: true,
			wantErr:       false,
		},
		{
			name:          "Role system enabled, has admin role",
			target:        RoleWriter,
			actual:        []Role{RoleAdmin},
			enableRoleSys: true,
			wantErr:       false,
		},
		{
			name:          "Role system enabled, does not have target role",
			target:        RoleWriter,
			actual:        []Role{RoleReader},
			enableRoleSys: true,
			wantErr:       true,
		},
		{
			name:          "Role system enabled, does not have admin role",
			target:        RoleAdmin,
			actual:        []Role{RoleReader, RoleWriter},
			enableRoleSys: true,
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgRouter.EnableRoleSystem = tt.enableRoleSys
			err := CheckGrants(tt.target, tt.actual)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckGrants() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
