package catalog

import (
	"fmt"

	"github.com/pg-sharding/spqr/pkg/config"
)

const (
	RoleReader = "reader"
	RoleWriter = "writer"
	RoleAdmin  = "admin"
)

// CheckGrants checks if the given role has the necessary grants to access the specified database.
//
// Parameters:
//   - target (Role): The role to check.
//   - rule (*FrontendRule): The rule to check.
//
// Returns:
//   - error: An error if the role does not have the necessary grants.
func CheckGrants(target string, rule *config.FrontendRule) error {
	// TODO use some common flag
	if !config.RouterConfig().EnableRoleSystem {
		return nil
	}

	if len(config.RolesConfig().TableGroups) > 1 {
		return fmt.Errorf("only one table group is supported")
	}


	var allowedUsers []string
	switch target {
	case RoleReader:
		allowedUsers = config.RolesConfig().TableGroups[0].Readers
	case RoleWriter:
		allowedUsers = config.RolesConfig().TableGroups[0].Writers
	case RoleAdmin:
		allowedUsers = config.RolesConfig().TableGroups[0].Admins
	default:
		return fmt.Errorf("unknown role %s", target)
	}

	for _, user := range allowedUsers {
		if user == rule.Usr {
			return nil
		}
	}

	return fmt.Errorf("permission denied for user=%s dbname=%s", rule.Usr, rule.DB)
}
