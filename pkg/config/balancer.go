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

type Balancer struct {
	LogLevel string `json:"log_level" toml:"log_level" yaml:"log_level"` // TODO usage

	InstallationDBName      string `json:"installation_db_name" toml:"installation_db_name" yaml:"installation_db_name"`
	InstallationTableName   string `json:"installation_table_name" toml:"installation_table_name" yaml:"installation_table_name"`
	InstallationShardingKey string `json:"installation_sharding_key" toml:"installation_sharding_key" yaml:"installation_sharding_key"`
	InstallationUserName    string `json:"installation_user_name" toml:"installation_user_name" yaml:"installation_user_name"`
	InstallationPassword    string `json:"installation_password" toml:"installation_password" yaml:"installation_password"`
	InstallationSSLMode     string `json:"installation_ssl_mode" toml:"installation_ssl_mode" yaml:"installation_ssl_mode"`
	InstallationMaxRetries  int    `json:"installation_max_retries" toml:"installation_max_retries" yaml:"installation_max_retries"`

	CoordinatorAddress    string `json:"coordinator_address" toml:"coordinator_address" yaml:"coordinator_address"`
	CoordinatorMaxRetries string `json:"coordinator_max_retries" toml:"coordinator_max_retries" yaml:"coordinator_max_retries"`

	DatabaseHosts      []string `json:"database_hosts" toml:"database_hosts" yaml:"database_hosts"`
	DatabasePassword   string   `json:"database_password" toml:"database_password" yaml:"database_password"`
	DatabasePort       int      `json:"database_port" toml:"database_port" yaml:"database_port"`
	DatabaseMaxRetries int      `json:"database_max_retries" toml:"database_max_retries" yaml:"database_max_retries"`

	TLS TLSConfig `json:"tls" yaml:"tls" toml:"tls"`
}

var cfgBalancer Balancer

func LoadBalancerCfg(cfgPath string) error {
	file, err := os.Open(cfgPath)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := initBalancerConfig(file, cfgPath); err != nil {
		return err
	}

	configBytes, err := json.MarshalIndent(cfgBalancer, "", "  ")
	if err != nil {
		return err
	}

	log.Println("Running config:", string(configBytes))
	return nil
}

func initBalancerConfig(file *os.File, filepath string) error {
	if strings.HasSuffix(filepath, ".toml") {
		_, err := toml.NewDecoder(file).Decode(&cfgBalancer)
		return err
	}
	if strings.HasSuffix(filepath, ".yaml") {
		return yaml.NewDecoder(file).Decode(&cfgBalancer)
	}
	if strings.HasSuffix(filepath, ".json") {
		return json.NewDecoder(file).Decode(&cfgBalancer)
	}
	return fmt.Errorf("unknown config format type: %s. Use .toml, .yaml or .json suffix in filename", filepath)
}

func BalancerConfig() *Balancer {
	return &cfgBalancer
}
