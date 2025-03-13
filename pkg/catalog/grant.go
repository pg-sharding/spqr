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

type GrantChecker interface {
	CheckGrants(target string, rule *config.FrontendRule) error
}

type FakeGrantsChecker struct{}

func (f *FakeGrantsChecker) CheckGrants(target string, rule *config.FrontendRule) error {
	// Always allow access in the fake checker
	return nil
}

func NewGrantsChecker(enableRoleSystem bool, tableGroups []config.TableGroup) (GrantChecker, error) {
	if !enableRoleSystem {
		return &FakeGrantsChecker{}, nil
	}

	if enableRoleSystem && len(tableGroups) == 0 {
		return nil, fmt.Errorf("role system is enabled but no table groups are defined")
	}

	if len(tableGroups) > 1 {
		return nil, fmt.Errorf("multiple table groups are not supported")
	}

	return NewCatalog(tableGroups[0].Readers, tableGroups[0].Writers, tableGroups[0].Admins), nil
}

type Catalog struct {
	readers map[string]bool
	writers map[string]bool
	admins  map[string]bool
}

func NewCatalog(readers, writers, admins []string) GrantChecker {
	readersMap := make(map[string]bool)
	for _, reader := range readers {
		readersMap[reader] = true
	}

	writersMap := make(map[string]bool)
	for _, writer := range writers {
		writersMap[writer] = true
	}

	adminsMap := make(map[string]bool)
	for _, admin := range admins {
		adminsMap[admin] = true
	}

	return &Catalog{
		readers: readersMap,
		writers: writersMap,
		admins:  adminsMap,
	}
}

// CheckGrants checks if the given role has the necessary grants to access the specified database.
//
// Parameters:
//   - target (Role): The role to check.
//   - rule (*FrontendRule): The rule to check.
//
// Returns:
//   - error: An error if the role does not have the necessary grants.
func (c *Catalog) CheckGrants(target string, rule *config.FrontendRule) error {
	if rule == nil {
		return fmt.Errorf("rule is nil")
	}

	if c.admins[rule.Usr] {
		return nil
	}

	if target == RoleReader && c.readers[rule.Usr] {
		return nil
	}

	if target == RoleWriter && c.writers[rule.Usr] {
		return nil
	}

	return fmt.Errorf("permission denied for user=%s dbname=%s", rule.Usr, rule.DB)
}
