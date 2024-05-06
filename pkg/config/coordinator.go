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

var cfgCoordinator Coordinator

type Coordinator struct {
	LogLevel             string     `json:"log_level" toml:"log_level" yaml:"log_level"`
	QdbAddr              string     `json:"qdb_addr" toml:"qdb_addr" yaml:"qdb_addr"`
	CoordinatorPort      string     `json:"coordinator_port" toml:"coordinator_port" yaml:"coordinator_port"`
	GrpcApiPort          string     `json:"grpc_api_port" toml:"grpc_api_port" yaml:"grpc_api_port"`
	Host                 string     `json:"host" toml:"host" yaml:"host"`
	Auth                 *AuthCfg   `json:"auth" toml:"auth" yaml:"auth"`
	FrontendTLS          *TLSConfig `json:"frontend_tls" yaml:"frontend_tls" toml:"frontend_tls"`
	ShardDataCfg         string     `json:"shard_data" toml:"shard_data" yaml:"shard_data"`
	UseSystemdNotifier   bool       `json:"use_systemd_notifier" toml:"use_systemd_notifier" yaml:"use_systemd_notifier"`
	SystemdNotifierDebug bool       `json:"systemd_notifier_debug" toml:"systemd_notifier_debug" yaml:"systemd_notifier_debug"`
}

func LoadCoordinatorCfg(cfgPath string) error {
	file, err := os.Open(cfgPath)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := initCoordinatorConfig(file, cfgPath); err != nil {
		return err
	}

	configBytes, err := json.MarshalIndent(&cfgCoordinator, "", "  ")
	if err != nil {
		return err
	}

	log.Println("Running config:", string(configBytes))
	return nil
}

func initCoordinatorConfig(file *os.File, filepath string) error {
	if strings.HasSuffix(filepath, ".toml") {
		_, err := toml.NewDecoder(file).Decode(&cfgCoordinator)
		return err
	}
	if strings.HasSuffix(filepath, ".yaml") {
		return yaml.NewDecoder(file).Decode(&cfgCoordinator)
	}
	if strings.HasSuffix(filepath, ".json") {
		return json.NewDecoder(file).Decode(&cfgCoordinator)
	}
	return fmt.Errorf("unknown config format type: %s. Use .toml, .yaml or .json suffix in filename", filepath)
}

func CoordinatorConfig() *Coordinator {
	return &cfgCoordinator
}
