package config

import (
	"encoding/json"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

type BalancerCfg struct {
	LogLevel string `json:"log_level" toml:"log_level" yaml:"log_level"` // TODO usage

	InstallationDBName     string `json:"installation_db_name" toml:"installation_db_name" yaml:"installation_db_name"`
	InstallationTableName  string `json:"installation_table_name" toml:"installation_table_name" yaml:"installation_table_name"`
	InstallationUserName   string `json:"installation_user_name" toml:"installation_user_name" yaml:"installation_user_name"`
	InstallationPassword   string `json:"installation_password" toml:"installation_password" yaml:"installation_password"`
	InstallationSSLMode    string `json:"installation_ssl_mode" toml:"installation_ssl_mode" yaml:"installation_ssl_mode"`
	InstallationMaxRetries int    `json:"installation_max_retries" toml:"installation_max_retries" yaml:"installation_max_retries"`

	CoordinatorAddress    string `json:"coordinator_address" toml:"coordinator_address" yaml:"coordinator_address"`
	CoordinatorMaxRetries string `json:"coordinator_max_retries" toml:"coordinator_max_retries" yaml:"coordinator_max_retries"`

	DatabaseHosts      []string `json:"database_hosts" toml:"database_hosts" yaml:"database_hosts"`
	DatabasePassword   string   `json:"database_password" toml:"database_password" yaml:"database_password"`
	DatabasePort       int      `json:"database_port" toml:"database_port" yaml:"database_port"`
	DatabaseMaxRetries int      `json:"database_max_retries" toml:"database_max_retries" yaml:"database_max_retries"`
}

var cfgBalancer BalancerCfg

func LoadBalancerCfg(cfgPath string) error {
	file, err := os.Open(cfgPath)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := yaml.NewDecoder(file).Decode(&cfgBalancer); err != nil {
		return err
	}

	configBytes, err := json.MarshalIndent(cfgBalancer, "", "  ")
	if err != nil {
		return err
	}

	log.Println("Running config:", string(configBytes))
	return nil
}

func BalancerConfig() *BalancerCfg {
	return &cfgBalancer
}
