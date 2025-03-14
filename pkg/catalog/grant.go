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

// GC is a singleton object of GrantChecker.
var GC GrantChecker = &FakeChecker{}

// Reload reloads the grants checker.
//
// Parameters:
//   - enableRoleSystem (bool): Enable the role system.
//   - tableGroups ([]TableGroup): The table groups.
//
// Returns:
//   - error: An error if any occurred during the reload process.
func Reload(enableRoleSystem bool, tableGroups []config.TableGroup) error {
	grantsChecker, err := NewGrantsChecker(enableRoleSystem, tableGroups)
	if err != nil {
		return err
	}

	GC = grantsChecker
	return nil
}

// GrantChecker is an interface for checking grants.
type GrantChecker interface {
	CheckGrants(target string, rule *config.FrontendRule) error
}

// NewGrantsChecker creates a new GrantsChecker.
//
// Parameters:
//   - enableRoleSystem (bool): Enable the role system.
//   - tableGroups ([]TableGroup): The table groups.
//
// Returns:
//   - GrantChecker: The new GrantsChecker.
//   - error: An error if any occurred during the creation process.
func NewGrantsChecker(enableRoleSystem bool, tableGroups []config.TableGroup) (GrantChecker, error) {
	if !enableRoleSystem {
		return &FakeChecker{}, nil
	}

	if enableRoleSystem && len(tableGroups) == 0 {
		return nil, fmt.Errorf("role system is enabled but no table groups are defined")
	}

	if len(tableGroups) > 1 {
		return nil, fmt.Errorf("multiple table groups are not supported")
	}

	readersMap := make(map[string]bool)
	for _, reader := range tableGroups[0].Readers {
		readersMap[reader] = true
	}

	writersMap := make(map[string]bool)
	for _, writer := range tableGroups[0].Writers {
		writersMap[writer] = true
	}

	adminsMap := make(map[string]bool)
	for _, admin := range tableGroups[0].Admins {
		adminsMap[admin] = true
	}

	return &RealChecker{
		readers: readersMap,
		writers: writersMap,
		admins:  adminsMap,
	}, nil
}

// FakeChecker is a fake implementation.
type FakeChecker struct{}

// CheckGrants fake implementation.
func (f *FakeChecker) CheckGrants(target string, rule *config.FrontendRule) error {
	// Always allow access in the fake checker
	return nil
}

// RealChecker is a real implementation of the GrantChecker interface.
type RealChecker struct {
	readers map[string]bool
	writers map[string]bool
	admins  map[string]bool
}

// CheckGrants checks if the given role has the necessary grants to access the specified database.
//
// Parameters:
//   - target (Role): The role to check.
//   - rule (*FrontendRule): The rule to check.
//
// Returns:
//   - error: An error if the role does not have the necessary grants.
func (c *RealChecker) CheckGrants(target string, rule *config.FrontendRule) error {
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
