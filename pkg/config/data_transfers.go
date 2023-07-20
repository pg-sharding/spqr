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

type DatatransferConnections struct {
	ShardsData map[string]*ShardConnect `json:"shards" toml:"shards" yaml:"shards"`
}

type ShardConnect struct {
	Host     string `json:"host" toml:"host" yaml:"host"`
	Port     string `json:"port" toml:"port" yaml:"port"`
	DB       string `json:"db" toml:"db" yaml:"db"`
	User     string `json:"user" toml:"user" yaml:"user"`
	Password string `json:"password" toml:"password" yaml:"password"`
}

func LoadShardDataCfg(cfgPath string) (*DatatransferConnections, error) {
	var cfg DatatransferConnections
	file, err := os.Open(cfgPath)
	if err != nil {
		return &cfg, err
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("failed to close config file: %v", err)
		}
	}(file)

	if err := initShardDataConfig(file, &cfg); err != nil {
		return &cfg, err
	}

	configBytes, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return &cfg, err
	}

	fmt.Println("Running config:", string(configBytes))

	return &cfg, nil
}

func initShardDataConfig(file *os.File, cfg *DatatransferConnections) error {
	if strings.HasSuffix(file.Name(), ".toml") {
		_, err := toml.NewDecoder(file).Decode(&cfg)
		return err
	}
	if strings.HasSuffix(file.Name(), ".yaml") {
		return yaml.NewDecoder(file).Decode(&cfg)
	}
	if strings.HasSuffix(file.Name(), ".json") {
		return json.NewDecoder(file).Decode(&cfg)
	}
	return fmt.Errorf("unknown config format type: %s. Use .toml, .yaml or .json suffix in filename", file.Name())
}
