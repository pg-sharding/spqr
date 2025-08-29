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

const defaultBalancerTimeout = 60

type Balancer struct {
	LogLevel string `json:"log_level" toml:"log_level" yaml:"log_level"` // TODO usage

	CoordinatorAddress string `json:"coordinator_address" toml:"coordinator_address" yaml:"coordinator_address"`

	ShardsConfig string `json:"shards_config" yaml:"shards_config" toml:"shards_config"`

	// TODO set default values (probably the type needs to be de-exported)
	CpuThreshold   float64 `json:"cpu_threshold" yaml:"cpu_threshold" toml:"cpu_threshold"`
	SpaceThreshold float64 `json:"space_threshold" yaml:"space_threshold" toml:"space_threshold"`

	StatIntervalSec int `json:"stat_interval_sec" yaml:"stat_interval_sec" toml:"stat_interval_sec"`

	MaxMoveCount int `json:"max_move_count" yaml:"max_move_count" toml:"max_move_count"`
	KeysPerMove  int `json:"keys_per_move" yaml:"keys_per_move" toml:"keys_per_move"`

	TimeoutSec int `json:"timeout" yaml:"timeout" toml:"timeout"`
}

var cfgBalancer Balancer

// LoadBalancerCfg loads the balancer configuration from the specified file path.
//
// Parameters:
//   - cfgPath (string): The path of the configuration file.
//
// Returns:
//   - string: JSON-formatted config
//   - error: an error if any occurred during the loading process.
func LoadBalancerCfg(cfgPath string) (string, error) {
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

	if err := initBalancerConfig(file, cfgPath); err != nil {
		return "", err
	}

	if cfgBalancer.TimeoutSec == 0 {
		cfgBalancer.TimeoutSec = defaultBalancerTimeout
	}

	configBytes, err := json.MarshalIndent(cfgBalancer, "", "  ")
	if err != nil {
		return "", err
	}

	return string(configBytes), nil
}

// initBalancerConfig initializes the balancer configuration based on the file content and file format.
//
// Parameters:
//   - file (*os.File): the file containing the configuration data.
//   - filepath (string): the path of the configuration file.
//
// Returns:
//   - error: an error if any occurred during the initialization process.
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

// BalancerConfig returns a pointer to the Balancer configuration.
//
// Returns:
//   - *Balancer: a pointer to the Balancer configuration.
func BalancerConfig() *Balancer {
	return &cfgBalancer
}
