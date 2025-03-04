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
	if !config.RouterConfig().EnableRoleSystem {
		return nil
	}

	for _, g := range rule.Grants {
		if string(g) == target || g == RoleAdmin {
			return nil
		}
	}

	return fmt.Errorf("permission denied for user=%s dbname=%s", rule.Usr, rule.DB)
}
