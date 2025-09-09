package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"gopkg.in/yaml.v2"
)

var cfgCoordinator Coordinator

type Coordinator struct {
	LogLevel             string          `json:"log_level" toml:"log_level" yaml:"log_level"`
	PrettyLogging        bool            `json:"pretty_logging" toml:"pretty_logging" yaml:"pretty_logging"`
	QdbAddr              string          `json:"qdb_addr" toml:"qdb_addr" yaml:"qdb_addr"`
	CoordinatorPort      string          `json:"coordinator_port" toml:"coordinator_port" yaml:"coordinator_port"`
	GrpcApiPort          string          `json:"grpc_api_port" toml:"grpc_api_port" yaml:"grpc_api_port"`
	Host                 string          `json:"host" toml:"host" yaml:"host"`
	FrontendTLS          *TLSConfig      `json:"frontend_tls" yaml:"frontend_tls" toml:"frontend_tls"`
	FrontendRules        []*FrontendRule `json:"frontend_rules" toml:"frontend_rules" yaml:"frontend_rules"`
	ShardDataCfg         string          `json:"shard_data" toml:"shard_data" yaml:"shard_data"`
	UseSystemdNotifier   bool            `json:"use_systemd_notifier" toml:"use_systemd_notifier" yaml:"use_systemd_notifier"`
	SystemdNotifierDebug bool            `json:"systemd_notifier_debug" toml:"systemd_notifier_debug" yaml:"systemd_notifier_debug"`
	IterationTimeout     time.Duration   `json:"iteration_timeout" toml:"iteration_timeout" yaml:"iteration_timeout"`
	LockIterationTimeout time.Duration   `json:"lock_iteration_timeout" toml:"lock_iteration_timeout" yaml:"lock_iteration_timeout"`
	EnableRoleSystem     bool            `json:"enable_role_system" toml:"enable_role_system" yaml:"enable_role_system"`
	RolesFile            string          `json:"roles_file" toml:"roles_file" yaml:"roles_file"`

	EtcdMaxSendBytes int `json:"etcd_max_send_bytes" toml:"etcd_max_send_bytes" yaml:"etcd_max_send_bytes"`
}

// LoadCoordinatorCfg loads the coordinator configuration from the specified file path.
//
// Parameters:
//   - cfgPath (string): The path of the configuration file.
//
// Returns:
//   - string: JSON-formatted config
//   - error: An error if any occurred during the loading process.
func LoadCoordinatorCfg(cfgPath string) (string, error) {
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

	if err := initCoordinatorConfig(file, cfgPath); err != nil {
		return "", err
	}

	configBytes, err := json.MarshalIndent(&cfgCoordinator, "", "  ")
	if err != nil {
		return "", err
	}

	return string(configBytes), nil
}

// initCoordinatorConfig initializes the coordinator configuration based on the file content and file format.
//
// Parameters:
//   - file (*os.File): the file containing the configuration data.
//   - filepath (string): the path of the configuration file.
//
// Returns:
//   - error: an error if any occurred during the initialization process.
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

// CoordinatorConfig returns a pointer to the Coordinator configuration.
//
// Returns:
//   - *Coordinator: a pointer to the Coordinator configuration.
func CoordinatorConfig() *Coordinator {
	return &cfgCoordinator
}
