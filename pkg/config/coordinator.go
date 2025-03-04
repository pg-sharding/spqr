package config

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

var cfgCoordinator Coordinator

type Coordinator struct {
	LogLevel             string        `json:"log_level" toml:"log_level" yaml:"log_level"`
	QdbAddr              string        `json:"qdb_addr" toml:"qdb_addr" yaml:"qdb_addr"`
	CoordinatorPort      string        `json:"coordinator_port" toml:"coordinator_port" yaml:"coordinator_port"`
	GrpcApiPort          string        `json:"grpc_api_port" toml:"grpc_api_port" yaml:"grpc_api_port"`
	Host                 string        `json:"host" toml:"host" yaml:"host"`
	Auth                 *AuthCfg      `json:"auth" toml:"auth" yaml:"auth"`
	FrontendTLS          *TLSConfig    `json:"frontend_tls" yaml:"frontend_tls" toml:"frontend_tls"`
	ShardDataCfg         string        `json:"shard_data" toml:"shard_data" yaml:"shard_data"`
	UseSystemdNotifier   bool          `json:"use_systemd_notifier" toml:"use_systemd_notifier" yaml:"use_systemd_notifier"`
	SystemdNotifierDebug bool          `json:"systemd_notifier_debug" toml:"systemd_notifier_debug" yaml:"systemd_notifier_debug"`
	IterationTimeout     time.Duration `json:"iteration_timeout" toml:"iteration_timeout" yaml:"iteration_timeout"`
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
	var ccfg Coordinator
	file, err := os.Open(cfgPath)
	if err != nil {
		cfgCoordinator = ccfg
		return "", err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("failed to close config file: %v", err)
		}
	}(file)

	if err := initConfig(file, &ccfg); err != nil {
		cfgCoordinator = ccfg
		return "", err
	}

	configBytes, err := json.MarshalIndent(&cfgCoordinator, "", "  ")
	if err != nil {
		cfgCoordinator = ccfg
		return "", err
	}

	return string(configBytes), nil
}

// CoordinatorConfig returns a pointer to the Coordinator configuration.
//
// Returns:
//   - *Coordinator: a pointer to the Coordinator configuration.
func CoordinatorConfig() *Coordinator {
	return &cfgCoordinator
}
