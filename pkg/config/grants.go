package config

import "fmt"

type Role string

const (
	RoleReader = Role("reader")
	RoleWriter = Role("writer")
	RoleAdmin  = Role("admin")
)

// CheckGrants checks if the given role has the necessary grants to access the specified database.
//
// Parameters:
//   - target (Role): The role to check.
//   - rule (*FrontendRule): The rule to check.
//
// Returns:
//   - error: An error if the role does not have the necessary grants.
func CheckGrants(target Role, rule *FrontendRule) error {
	if !RouterConfig().EnableRoleSystem {
		return nil
	}

	for _, g := range rule.Grants {
		if g == target || g == RoleAdmin {
			return nil
		}
	}

	return fmt.Errorf("permission denied for user=%s dbname=%s", rule.Usr, rule.DB)
}
