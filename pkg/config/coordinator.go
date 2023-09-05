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
	LogLevel     string `json:"log_level" toml:"log_level" yaml:"log_level"`
	QdbAddr      string `json:"qdb_addr" toml:"qdb_addr" yaml:"qdb_addr"`
	HttpAddr     string `json:"http_addr" toml:"http_addr" yaml:"http_addr"`
	Addr         string `json:"addr" toml:"addr" yaml:"addr"`
	ShardDataCfg string `json:"shard_data" toml:"shard_data" yaml:"shard_data"`
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
