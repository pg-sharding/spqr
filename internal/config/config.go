package config

import (
	"encoding/json"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

type Spqr struct {
	LogLevel string `json:"log_level" toml:"log_level" yaml:"log_level"` // TODO usage
	HttpAddr string `json:"http_addr" toml:"http_addr" yaml:"http_addr"`
	Addr     string `json:"addr" toml:"addr" yaml:"addr"`
	ADMAddr  string `json:"adm_addr" toml:"adm_addr" yaml:"adm_addr"` // Console Addr
	PROTO    string `json:"proto" toml:"proto" yaml:"proto"`

	QRouterCfg   QrouterConfig `json:"qrouter" toml:"qrouter" yaml:"qrouter"`
	ExecuterCfg  ExecuterCfg   `json:"executer" toml:"executer" yaml:"executer"`
	RouterConfig RouterConfig  `json:"router" toml:"router" yaml:"router"`
}

var cfg Spqr

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

func Get() *Spqr {
	return &cfg
}
