package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"gopkg.in/yaml.v2"
)

var cfgRoles Roles

type Roles struct {
	TableGroups []TableGroup `json:"table_groups" toml:"table_groups" yaml:"table_groups"`
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
	file, err := os.Open(cfgPath)
	if err != nil {
		return "", err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Printf("failed to close config file: %v", err)
		}
	}(file)

	if err := initRolesConfig(file, cfgPath); err != nil {
		return "", err
	}

	configBytes, err := json.MarshalIndent(&cfgRoles, "", "  ")
	if err != nil {
		return "", err
	}

	return string(configBytes), nil
}

// TODO use intiConfig
func initRolesConfig(file *os.File, filepath string) error {
	if strings.HasSuffix(filepath, ".toml") {
		_, err := toml.NewDecoder(file).Decode(&cfgRoles)
		return err
	}
	if strings.HasSuffix(filepath, ".yaml") {
		return yaml.NewDecoder(file).Decode(&cfgRoles)
	}
	if strings.HasSuffix(filepath, ".json") {
		return json.NewDecoder(file).Decode(&cfgRoles)
	}
	return fmt.Errorf("unknown config format type: %s. Use .toml, .yaml or .json suffix in filename", filepath)
}

// RolesConfig returns a pointer to the ACLs configuration.
//
// Returns:
//   - *Roles: a pointer to the Roles configuration.
func RolesConfig() *Roles {
	return &cfgRoles
}
