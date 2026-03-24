package config

var cfgRoles Roles

type Roles struct {
	TableGroups []TableGroup `json:"table_groups" toml:"table_groups" yaml:"table_groups"`
}

func (r *Roles) ApplyDefaults() {

}

func (r *Roles) PostProcess() error {
	return nil
}

type TableGroup struct {
	ID string `json:"id" toml:"id" yaml:"id"`
	// TODO support specific tables and table prefixes
	// Tables []string `json:"tables" toml:"tables" yaml:"tables"`
	Readers []string `json:"readers" toml:"readers" yaml:"readers"`
	Writers []string `json:"writers" toml:"writers" yaml:"writers"`
	Admins  []string `json:"admins" toml:"admins" yaml:"admins"`
}

// LoadRolesCfg loads the ACLs configuration from the specified file path.
//
// Parameters:
//   - cfgPath (string): The path of the configuration file.
//
// Returns:
//   - string: JSON-formatted config
//   - error: An error if any occurred during the loading process.
func LoadRolesCfg(cfgPath string) (string, error) {
	r := &Roles{}
	configStr, err := LoadConfig(cfgPath, r)
	if err != nil {
		return "", err
	}

	cfgRoles = *r
	return configStr, nil
}

// RolesConfig returns a pointer to the ACLs configuration.
//
// Returns:
//   - *Roles: a pointer to the Roles configuration.
func RolesConfig() *Roles {
	return &cfgRoles
}
