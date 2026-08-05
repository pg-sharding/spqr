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
	s := &DatatransferConnections{}
	configStr, err := LoadConfig(cfgPath, s)
	if err != nil {
		return nil, err
	}

	spqrlog.Zero.Debug().Str("config", configStr).Msg("got shard data config")

	return s, nil
}

// SplitHostPort splits a "host:port" string into host and port components.
// If no port is provided, defaults to "5432".
func SplitHostPort(hostPort string) (host, port string) {
	parts := strings.SplitN(hostPort, ":", 2)
	if len(parts) < 2 || parts[1] == "" {
		return parts[0], "5432"
	}
	return parts[0], parts[1]
}

func (sc *ShardConnect) GetConnStrings() []string {
	res := make([]string, len(sc.Hosts))
	for i, h := range sc.Hosts {
		hostname, port := SplitHostPort(h)
		res[i] = fmt.Sprintf("user=%s host=%s port=%s dbname=%s password=%s", sc.User, hostname, port, sc.DB, sc.Password)
	}
	return res
}

func (sc *ShardConnect) GetCombinedConnString() string {
	hosts := make([]string, len(sc.Hosts))
	ports := make([]string, len(sc.Hosts))
	for i, h := range sc.Hosts {
		hostname, port := SplitHostPort(h)
		hosts[i] = hostname
		ports[i] = port
	}
	return fmt.Sprintf("user=%s host=%s port=%s dbname=%s password=%s", sc.User, strings.Join(hosts, ","), strings.Join(ports, ","), sc.DB, sc.Password)
}

func AddTSA(connString, tsa string) string {
	return fmt.Sprintf("%s target_session_attrs=%s", connString, tsa)
}
