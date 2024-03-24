package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pg-sharding/spqr/router/statistics"
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

var cfgRouter Router

type Router struct {
	LogLevel string `json:"log_level" toml:"log_level" yaml:"log_level"`

	TimeQuantiles []float64 `json:"time_quantiles" toml:"time_quantiles" yaml:"time_quantiles"`

	Daemonize bool `json:"daemonize" toml:"daemonize" yaml:"daemonize"`

	MaintainParams bool `json:"maintain_params" toml:"maintain_params" yaml:"maintain_params"`
	WithJaeger     bool `json:"with_jaeger" toml:"with_jaeger" yaml:"with_jaeger"`
	PgprotoDebug   bool `json:"pgproto_debug" toml:"pgproto_debug" yaml:"pgproto_debug"`

	PidFileName string `json:"pid_filename" toml:"pid_filename" yaml:"pid_filename"`
	LogFileName string `json:"log_filename" toml:"log_filename" yaml:"log_filename"`

	Host             string `json:"host" toml:"host" yaml:"host"`
	RouterPort       string `json:"router_port" toml:"router_port" yaml:"router_port"`
	RouterROPort     string `json:"router_ro_port" toml:"router_ro_port" yaml:"router_ro_port"`
	AdminConsolePort string `json:"admin_console_port" toml:"admin_console_port" yaml:"admin_console_port"`
	GrpcApiPort      string `json:"grpc_api_port" toml:"grpc_api_port" yaml:"grpc_api_port"`

	WorldShardFallback bool `json:"world_shard_fallback" toml:"world_shard_fallback" yaml:"world_shard_fallback"`
	ShowNoticeMessages bool `json:"show_notice_messages" toml:"show_notice_messages" yaml:"show_notice_messages"`

	InitSQL          string            `json:"init_sql" toml:"init_sql" yaml:"init_sql"`
	MemqdbBackupPath string            `json:"memqdb_backup_path" toml:"memqdb_backup_path" yaml:"memqdb_backup_path"`
	MemqdbPersistent bool              `json:"memqdb_persistent" toml:"memqdb_persistent" yaml:"memqdb_persistent"`
	RouterMode       string            `json:"router_mode" toml:"router_mode" yaml:"router_mode"`
	JaegerUrl        string            `json:"jaeger_url" toml:"jaeger_url" yaml:"jaeger_url"`
	FrontendRules    []*FrontendRule   `json:"frontend_rules" toml:"frontend_rules" yaml:"frontend_rules"`
	Qr               QRouter           `json:"query_routing" toml:"query_routing" yaml:"query_routing"`
	FrontendTLS      *TLSConfig        `json:"frontend_tls" yaml:"frontend_tls" toml:"frontend_tls"`
	BackendRules     []*BackendRule    `json:"backend_rules" toml:"backend_rules" yaml:"backend_rules"`
	ShardMapping     map[string]*Shard `json:"shards" toml:"shards" yaml:"shards"`

	WorkloadFile      string `json:"workload_file" toml:"workload_file" yaml:"workload_file"`
	WorkloadBatchSize int    `json:"workload_batch_size" toml:"workload_batch_size" yaml:"workload_batch_size"`

	ReusePort bool `json:"reuse_port" toml:"reuse_port" yaml:"reuse_port"`

	WithCoordinator bool `json:"with_coordinator" toml:"with_coordinator" yaml:"with_coordinator"`

	UseSystemdNotifier   bool `json:"use_systemd_notifier" toml:"use_systemd_notifier" yaml:"use_systemd_notifier"`
	SystemdNotifierDebug bool `json:"systemd_notifier_debug" toml:"systemd_notifier_debug" yaml:"systemd_notifier_debug"`
}

type QRouter struct {
	MulticastUnroutableInsertStatement bool   `json:"multicast_unroutable_insert_statement" toml:"multicast_unroutable_insert_statement" yaml:"multicast_unroutable_insert_statement"`
	DefaultRouteBehaviour              string `json:"default_route_behaviour" toml:"default_route_behaviour" yaml:"default_route_behaviour"`
}

type BackendRule struct {
	DB                string              `json:"db" yaml:"db" toml:"db"`
	Usr               string              `json:"usr" yaml:"usr" toml:"usr"`
	AuthRules         map[string]*AuthCfg `json:"auth_rules" yaml:"auth_rules" toml:"auth_rules"` // TODO validate
	DefaultAuthRule   *AuthCfg            `json:"auth_rule" yaml:"auth_rule" toml:"auth_rule"`
	PoolDefault       bool                `json:"pool_default" yaml:"pool_default" toml:"pool_default"`
	ConnectionLimit   int                 `json:"connection_limit" yaml:"connection_limit" toml:"connection_limit"`
	ConnectionRetries int                 `json:"connection_retries" yaml:"connection_retries" toml:"connection_retries"`
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
	TargetSessionAttrsPS  = "prefer-standby"
	TargetSessionAttrsAny = "any"
)

type Shard struct {
	Hosts []string   `json:"hosts" toml:"hosts" yaml:"hosts"`
	Type  ShardType  `json:"type" toml:"type" yaml:"type"`
	TLS   *TLSConfig `json:"tls" yaml:"tls" toml:"tls"`
}

func LoadRouterCfg(cfgPath string) error {
	var rcfg Router
	file, err := os.Open(cfgPath)
	if err != nil {
		cfgRouter = rcfg
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("failed to close config file: %v", err)
		}
	}(file)

	if err := initRouterConfig(file, &rcfg); err != nil {
		cfgRouter = rcfg
		return err
	}

	statistics.InitStatistics(rcfg.TimeQuantiles)

	configBytes, err := json.MarshalIndent(rcfg, "", "  ")
	if err != nil {
		cfgRouter = rcfg
		return err
	}

	log.Println("Running config:", string(configBytes))
	cfgRouter = rcfg
	return nil
}

func initRouterConfig(file *os.File, cfgRouter *Router) error {
	if strings.HasSuffix(file.Name(), ".toml") {
		_, err := toml.NewDecoder(file).Decode(cfgRouter)
		return err
	}
	if strings.HasSuffix(file.Name(), ".yaml") {
		return yaml.NewDecoder(file).Decode(&cfgRouter)
	}
	if strings.HasSuffix(file.Name(), ".json") {
		return json.NewDecoder(file).Decode(&cfgRouter)
	}
	return fmt.Errorf("unknown config format type: %s. Use .toml, .yaml or .json suffix in filename", file.Name())
}

func RouterConfig() *Router {
	return &cfgRouter
}
