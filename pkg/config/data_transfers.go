package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"gopkg.in/yaml.v2"
)

type DatatransferConnections struct {
	ShardsData map[string]*ShardConnect `json:"shards" toml:"shards" yaml:"shards"`
}

type ShardConnect struct {
	Hosts    []string `json:"hosts" toml:"hosts" yaml:"hosts"`
	DB       string   `json:"db" toml:"db" yaml:"db"`
	User     string   `json:"usr" toml:"usr" yaml:"usr"`
	Password string   `json:"pwd" toml:"pwd" yaml:"pwd"`
}

func LoadShardDataCfg(cfgPath string) (*DatatransferConnections, error) {
	var cfg DatatransferConnections
	file, err := os.Open(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("could not open file \"%s\": %s", cfgPath, err)
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

	spqrlog.Zero.Debug().Str("running config: %s", string(configBytes))

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

func (s *ShardConnect) GetConnStrings() []string {
	res := make([]string, len(s.Hosts))
	for i, host := range s.Hosts {
		address := strings.Split(host, ":")[0]
		port := strings.Split(host, ":")[1]
		res[i] = fmt.Sprintf("user=%s host=%s port=%s dbname=%s password=%s", s.User, address, port, s.DB, s.Password)
	}
	return res
}
