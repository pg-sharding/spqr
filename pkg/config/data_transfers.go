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
	Hosts    []string   `json:"hosts" toml:"hosts" yaml:"hosts"`
	DB       string     `json:"db" toml:"db" yaml:"db"`
	User     string     `json:"usr" toml:"usr" yaml:"usr"`
	Password string     `json:"pwd" toml:"pwd" yaml:"pwd"`
	TLS      *TLSConfig `json:"tls,omitempty" toml:"tls,omitempty" yaml:"tls,omitempty"`
}

// LoadShardDataCfg loads the shard data configuration from the given file path.
//
// Parameters:
// - cfgPath (string): The path to the configuration file.
//
// Returns:
// - *DatatransferConnections: A pointer to the loaded DatatransferConnections struct.
// - error: An error if the file cannot be opened or the configuration cannot be initialized.
func LoadShardDataCfg(cfgPath string) (*DatatransferConnections, error) {
	var cfg DatatransferConnections
	file, err := os.Open(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("could not open file \"%s\": %s", cfgPath, err)
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Printf("failed to close config file: %v", err)
		}
	}(file)

	if err := initShardDataConfig(file, &cfg); err != nil {
		return &cfg, err
	}

	configBytes, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return &cfg, err
	}

	spqrlog.Zero.Debug().Str("config", string(configBytes)).Msg("got shard data config")

	return &cfg, nil
}

// initShardDataConfig initializes the shard data configuration from the given file.
//
// Parameters:
// - file (*os.File): The file containing the configuration data.
// - cfg (*DatatransferConnections): A pointer to the DatatransferConnections struct to be initialized.
//
// Returns:
// - error: An error if the file cannot be decoded or if the file format is unknown.
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
