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

	Host         string `json:"host" toml:"host" yaml:"host"`
	BalancerPort string `json:"balancer_port" toml:"balancer_port" yaml:"}"`

	CoordinatorAddress    string `json:"coordinator_address" toml:"coordinator_address" yaml:"coordinator_address"`
	CoordinatorMaxRetries string `json:"coordinator_max_retries" toml:"coordinator_max_retries" yaml:"coordinator_max_retries"`

	Shards DatatransferConnections `json:"shards" toml:"shards" yaml:"shards"`

	TLS TLSConfig `json:"tls" yaml:"tls" toml:"tls"`

	// TODO set default values (probably the type needs to be de-exported)
	CpuThreshold   float64 `json:"cpu_threshold" yaml:"cpu_threshold" toml:"cpu_threshold"`
	SpaceThreshold float64 `json:"space_threshold" yaml:"space_threshold" toml:"space_threshold"`

	StatIntervalSec int `json:"stat_interval_sec" yaml:"stat_interval_sec" toml:"stat_interval_sec"`

	MaxMoveCount int `json:"max_move_count" yaml:"max_move_count" toml:"max_move_count"`
	KeysPerMove  int `json:"keys_per_move" yaml:"keys_per_move" toml:"keys_per_move"`
}

var cfgBalancer Balancer

func LoadBalancerCfg(cfgPath string) error {
	file, err := os.Open(cfgPath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

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
