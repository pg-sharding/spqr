package config

import (
	"encoding/json"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

type SpqrConfig struct {
	LogLevel            string `json:"log_level" toml:"log_level" yaml:"log_level"` // TODO usage
	HttpAddr            string `json:"http_addr" toml:"http_addr" yaml:"http_addr"`
	WorldHttpAddr       string `json:"world_http_addr" toml:"world_http_addr" yaml:"world_http_addr"`
	CoordinatorHttpAddr string `json:"coordinator_http_addr" toml:"coordinator_http_addr" yaml:"coordinator_http_addr"`
	Addr                string `json:"addr" toml:"addr" yaml:"addr"`
	ADMAddr             string `json:"adm_addr" toml:"adm_addr" yaml:"adm_addr"` // Console Addr
	PROTO               string `json:"proto" toml:"proto" yaml:"proto"`
	DataFolder          string `json:"data_folder" toml:"data_folder" yaml:"data_folder"`

	QRouterCfg   QrouterConfig `json:"qrouter" toml:"qrouter" yaml:"qrouter"`
	ExecuterCfg  ExecuterCfg   `json:"executer" toml:"executer" yaml:"executer"`
	RouterConfig RouterConfig  `json:"router" toml:"router" yaml:"router"`
	JaegerConfig JaegerCfg     `json:"jaeger" toml:"jaeger" yaml:"jaeger"`
}

var cfg SpqrConfig

func Load(cfgPath string) error {
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

func Get() *SpqrConfig {
	return &cfg
}
