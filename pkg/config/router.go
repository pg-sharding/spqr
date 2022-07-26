package config

import (
	"encoding/json"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

type PoolMode string
type ShardType string
type RouterMode string

const (
	PoolModeSession     = PoolMode("SESSION")
	PoolModeTransaction = PoolMode("TRANSACTION")

	DataShard  = ShardType("DATA")
	WorldShard = ShardType("WORLD")

	LocalMode = RouterMode("LOCAL")
	ProxyMode = RouterMode("PROXY")
)

type Router struct {
	LogLevel string `json:"log_level" toml:"log_level" yaml:"log_level"`

	Host             string `json:"host" toml:"host" yaml:"host"`
	RouterPort       string `json:"router_port" toml:"router_port" yaml:"router_port"`
	AdminConsolePort string `json:"admin_console_port" toml:"admin_console_port" yaml:"admin_console_port"`
	GrpcApiPort      string `json:"grpc_api_port" toml:"grpc_api_port" yaml:"grpc_api_port"`

	WorldShardAddress  string `json:"world_shard_address" toml:"world_shard_address" yaml:"world_shard_address"`
	WorldShardFallback bool   `json:"world_shard_fallback" toml:"world_shard_fallback" yaml:"world_shard_fallback"`

	AutoConf      string            `json:"auto_conf" toml:"auto_conf" yaml:"auto_conf"`
	InitSQL       string            `json:"init_sql" toml:"init_sql" yaml:"init_sql"`
	RouterMode    string            `json:"router_mode" toml:"router_mode" yaml:"router_mode"`
	JaegerUrl     string            `json:"jaeger_url" toml:"jaeger_url" yaml:"jaeger_url"`
	FrontendRules []*FrontendRule   `json:"frontend_rules" toml:"frontend_rules" yaml:"frontend_rules"`
	FrontendTLS   *TLSConfig        `json:"frontend_tls" yaml:"frontend_tls" toml:"frontend_tls"`
	BackendRules  []*BackendRule    `json:"backend_rules" toml:"backend_rules" yaml:"backend_rules"`
	ShardMapping  map[string]*Shard `json:"shards" toml:"shards" yaml:"shards"`
}

type BackendRule struct {
	DB           string `json:"db" yaml:"db" toml:"db"`
	Usr          string `json:"usr" yaml:"usr" toml:"usr"`
	PoolDiscard  bool   `json:"pool_discard" yaml:"pool_discard" toml:"pool_discard"`
	PoolRollback bool   `json:"pool_rollback" yaml:"pool_rollback" toml:"pool_rollback"`
	PoolDefault  bool   `json:"pool_default" yaml:"pool_default" toml:"pool_default"`
}

type FrontendRule struct {
	DB                    string   `json:"db" yaml:"db" toml:"db"`
	Usr                   string   `json:"usr" yaml:"usr" toml:"usr"`
	AuthRule              *AuthCfg `json:"auth_rule" yaml:"auth_rule" toml:"auth_rule"` // TODO validate
	PoolMode              PoolMode `json:"pool_mode" yaml:"pool_mode" toml:"pool_mode"`
	PoolPreparedStatement bool     `json:"pool_prepared_statement" yaml:"pool_prepared_statement" toml:"pool_prepared_statement"`
	PoolDiscard           bool     `json:"pool_discard" yaml:"pool_discard" toml:"pool_discard"`
	PoolRollback          bool     `json:"pool_rollback" yaml:"pool_rollback" toml:"pool_rollback"`
	PoolDefault           bool     `json:"pool_default" yaml:"pool_default" toml:"pool_default"`
}

type Shard struct {
	DB    string     `json:"db" toml:"db" yaml:"db"`
	Usr   string     `json:"usr" toml:"usr" yaml:"usr"`
	Pwd   string     `json:"pwd" toml:"pwd" yaml:"pwd"`
	Hosts []string   `json:"hosts" toml:"hosts" yaml:"hosts"`
	Type  ShardType  `json:"type" toml:"type" yaml:"type"`
	TLS   *TLSConfig `json:"tls" yaml:"tls" toml:"tls"`
}

var cfgRouter Router

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

func RouterConfig() *Router {
	return &cfgRouter
}
