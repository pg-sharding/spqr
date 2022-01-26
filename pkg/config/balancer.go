package config

import (
	"encoding/json"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

type BalancerCfg struct {
	LogLevel   string `json:"log_level" toml:"log_level" yaml:"log_level"` // TODO usage
	DBName     string `json:"db_name" toml:"db_name" yaml:"db_name"` // TODO usage
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

/*
func BalancerConfig() *BalancerCfg {
	return &cfgBalancer
}
*/
