package config

import (
	"encoding/json"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

var cfg CoordinatorCfg

type CoordinatorCfg struct {
	LogLevel string `json:"log_level" toml:"log_level" yaml:"log_level"`
	QdbAddr  string `json:"qdb_addr" toml:"qdb_addr" yaml:"qdb_addr"`
	HttpAddr string `json:"http_addr" toml:"http_addr" yaml:"http_addr"`
	Addr     string `json:"addr" toml:"addr" yaml:"addr"`
}

func LoadCoordinatorCfg(cfgPath string) error {
	file, err := os.Open(cfgPath)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := yaml.NewDecoder(file).Decode(&cfg); err != nil {
		return err
	}

	configBytes, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	log.Println("Running config:", string(configBytes))
	return nil
}

func CoordinatorConfig() *CoordinatorCfg {
	return &cfg
}
