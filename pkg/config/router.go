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

	WorldShardFallback bool `json:"world_shard_fallback" toml:"world_shard_fallback" yaml:"world_shard_fallback"`
	ReplyShardMatch    bool `json:"reply_shard_match" toml:"reply_shard_match" yaml:"reply_shard_match"`

	AutoConf         string            `json:"auto_conf" toml:"auto_conf" yaml:"auto_conf"`
	InitSQL          string            `json:"init_sql" toml:"init_sql" yaml:"init_sql"`
	UnderCoordinator bool              `json:"under_coordinator" toml:"under_coordinator" yaml:"under_coordinator"`
	RouterMode       string            `json:"router_mode" toml:"router_mode" yaml:"router_mode"`
	JaegerUrl        string            `json:"jaeger_url" toml:"jaeger_url" yaml:"jaeger_url"`
	FrontendRules    []*FrontendRule   `json:"frontend_rules" toml:"frontend_rules" yaml:"frontend_rules"`
	FrontendTLS      *TLSConfig        `json:"frontend_tls" yaml:"frontend_tls" toml:"frontend_tls"`
	BackendRules     []*BackendRule    `json:"backend_rules" toml:"backend_rules" yaml:"backend_rules"`
	ShardMapping     map[string]*Shard `json:"shards" toml:"shards" yaml:"shards"`
}

type BackendRule struct {
	DB          string   `json:"db" yaml:"db" toml:"db"`
	Usr         string   `json:"usr" yaml:"usr" toml:"usr"`
	AuthRule    *AuthCfg `json:"auth_rule" yaml:"auth_rule" toml:"auth_rule"` // TODO validate
	PoolDefault bool     `json:"pool_default" yaml:"pool_default" toml:"pool_default"`
}

type FrontendRule struct {
	DB                    string   `json:"db" yaml:"db" toml:"db"`
	Usr                   string   `json:"usr" yaml:"usr" toml:"usr"`
	AuthRule              *AuthCfg `json:"auth_rule" yaml:"auth_rule" toml:"auth_rule"` // TODO validate
	PoolMode              PoolMode `json:"pool_mode" yaml:"pool_mode" toml:"pool_mode"`
	PoolDiscard           bool     `json:"pool_discard" yaml:"pool_discard" toml:"pool_discard"`
	PoolRollback          bool     `json:"pool_rollback" yaml:"pool_rollback" toml:"pool_rollback"`
	PoolPreparedStatement bool     `json:"pool_prepared_statement" yaml:"pool_prepared_statement" toml:"pool_prepared_statement"`
	PoolDefault           bool     `json:"pool_default" yaml:"pool_default" toml:"pool_default"`
}

const (
	TargetSessionAttrsRW  = "read-write"
	TargetSessionAttrsRO  = "read-only"
	TargetSessionAttrsAny = "any"
)

type Shard struct {
	TargetSessionAttrs string     `json:"target_session_attrs" toml:"target_session_attrs" yaml:"target_session_attrs"`
	Hosts              []string   `json:"hosts" toml:"hosts" yaml:"hosts"`
	Type               ShardType  `json:"type" toml:"type" yaml:"type"`
	TLS                *TLSConfig `json:"tls" yaml:"tls" toml:"tls"`
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
			log.Fatalf("failed to close confi file: %v", err)
		}
	}(file)

	if err := initRouterConfig(file, cfgPath); err != nil {
		return err
	}

	configBytes, err := json.MarshalIndent(cfgRouter, "", "  ")
	if err != nil {
		return err
	}

	log.Println("Running config:", string(configBytes))
	return nil
}

func initRouterConfig(file *os.File, filepath string) error {
	if strings.HasSuffix(filepath, ".toml") {
		_, err := toml.NewDecoder(file).Decode(&cfgRouter)
		return err
	}
	if strings.HasSuffix(filepath, ".yaml") {
		return yaml.NewDecoder(file).Decode(&cfgRouter)
	}
	if strings.HasSuffix(filepath, ".json") {
		return json.NewDecoder(file).Decode(&cfgRouter)
	}
	return fmt.Errorf("unknown config format type: %s. Use .toml, .yaml or .json suffix in filename", filepath)
}

func RouterConfig() *Router {
	return &cfgRouter
}
