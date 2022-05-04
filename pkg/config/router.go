package config

import (
	"encoding/json"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

type RouterCfg struct {
	LogLevel      string `json:"log_level" toml:"log_level" yaml:"log_level"` // TODO usage
	HttpAddr      string `json:"http_addr" toml:"http_addr" yaml:"http_addr"`
	WorldHttpAddr string `json:"world_http_addr" toml:"world_http_addr" yaml:"world_http_addr"`
	Addr          string `json:"addr" toml:"addr" yaml:"addr"`
	ADMAddr       string `json:"adm_addr" toml:"adm_addr" yaml:"adm_addr"` // Console Addr
	Proto         string `json:"proto" toml:"proto" yaml:"proto"`
	AutoConf      string `json:"auto_conf" toml:"auto_conf" yaml:"auto_conf"`
	InitSQL       string `json:"init_sql" toml:"init_sql" yaml:"init_sql"`

	QRouterCfg   QrouterConfig `json:"qrouter" toml:"qrouter" yaml:"qrouter"`
	ExecuterCfg  ExecuterCfg   `json:"executer" toml:"executer" yaml:"executer"`
	RulesConfig  RulesCfg      `json:"rules" toml:"rules" yaml:"rules"`
	JaegerConfig JaegerCfg     `json:"jaeger" toml:"jaeger" yaml:"jaeger"`
}

var cfgRouter RouterCfg

func LoadRouterCfg(cfgPath string) error {
	file, err := os.Open(cfgPath)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	if err := yaml.NewDecoder(file).Decode(&cfgRouter); err != nil {
		return err
	}

	configBytes, err := json.MarshalIndent(cfgRouter, "", "  ")
	if err != nil {
		return err
	}

	log.Println("Running config:", string(configBytes))
	return nil
}

func RouterConfig() *RouterCfg {
	return &cfgRouter
}
