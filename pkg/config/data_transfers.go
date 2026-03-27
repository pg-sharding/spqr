package config

import (
	"fmt"
	"strings"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type DatatransferConnections struct {
	ShardsData map[string]*ShardConnect `json:"shards" toml:"shards" yaml:"shards"`
}

var _ Config = &DatatransferConnections{}

func (dc *DatatransferConnections) ApplyDefaults() {

}

func (dc *DatatransferConnections) PostProcess() error {
	return nil
}

type ShardConnect struct {
	Hosts    []string `json:"hosts" toml:"hosts" yaml:"hosts"`
	DB       string   `json:"db" toml:"db" yaml:"db"`
	User     string   `json:"usr" toml:"usr" yaml:"usr"`
	Password string   `json:"pwd" toml:"pwd" yaml:"pwd"`
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
	s := &DatatransferConnections{}
	configStr, err := LoadConfig(cfgPath, s)
	if err != nil {
		return nil, err
	}

	spqrlog.Zero.Debug().Str("config", configStr).Msg("got shard data config")

	return s, nil
}

func (sc *ShardConnect) GetConnStrings() []string {
	res := make([]string, len(sc.Hosts))
	for i, h := range sc.Hosts {
		hostname := strings.Split(h, ":")[0]
		port := strings.Split(h, ":")[1]
		res[i] = fmt.Sprintf("user=%s host=%s port=%s dbname=%s password=%s", sc.User, hostname, port, sc.DB, sc.Password)
	}
	return res
}
