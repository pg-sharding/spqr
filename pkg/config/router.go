package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/router/statistics"
	"gopkg.in/yaml.v2"
)

type PoolMode string
type ShardType string
type RouterMode string
type DefaultRouteBehaviour string

const (
	PoolModeSession     = PoolMode("SESSION")
	PoolModeTransaction = PoolMode("TRANSACTION")

	DataShard  = ShardType("DATA")
	WorldShard = ShardType("WORLD")

	LocalMode = RouterMode("LOCAL")
	ProxyMode = RouterMode("PROXY")

	DefaultRouteBehaviourBlock = DefaultRouteBehaviour("BLOCK")
	DefaultRouteBehaviourAllow = DefaultRouteBehaviour("ALLOW")
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

	AvailabilityZone           string `json:"availability_zone" toml:"availability_zone" yaml:"availability_zone"`
	PreferSameAvailabilityZone bool   `json:"prefer_same_availability_zone" toml:"prefer_same_availability_zone" yaml:"prefer_same_availability_zone"`

	Host             string `json:"host" toml:"host" yaml:"host"`
	RouterPort       string `json:"router_port" toml:"router_port" yaml:"router_port"`
	RouterROPort     string `json:"router_ro_port" toml:"router_ro_port" yaml:"router_ro_port"`
	AdminConsolePort string `json:"admin_console_port" toml:"admin_console_port" yaml:"admin_console_port"`
	GrpcApiPort      string `json:"grpc_api_port" toml:"grpc_api_port" yaml:"grpc_api_port"`

	WorldShardFallback bool `json:"world_shard_fallback" toml:"world_shard_fallback" yaml:"world_shard_fallback"`
	ShowNoticeMessages bool `json:"show_notice_messages" toml:"show_notice_messages" yaml:"show_notice_messages"`

	InitSQL            string `json:"init_sql" toml:"init_sql" yaml:"init_sql"`
	UseInitSQL         bool   `json:"use_init_sql" toml:"use_init_sql" yaml:"use_init_sql"`
	UseCoordinatorInit bool   `json:"use_coordinator_init" toml:"use_coordinator_init" yaml:"use_coordinator_init"`

	/* default  */
	DefaultTSA string `json:"default_target_session_attrs" toml:"default_target_session_attrs" yaml:"default_target_session_attrs"`

	MemqdbBackupPath       string            `json:"memqdb_backup_path" toml:"memqdb_backup_path" yaml:"memqdb_backup_path"`
	MemqdbPersistent       bool              `json:"memqdb_persistent" toml:"memqdb_persistent" yaml:"memqdb_persistent"`
	RouterMode             string            `json:"router_mode" toml:"router_mode" yaml:"router_mode"`
	JaegerUrl              string            `json:"jaeger_url" toml:"jaeger_url" yaml:"jaeger_url"`
	FrontendRules          []*FrontendRule   `json:"frontend_rules" toml:"frontend_rules" yaml:"frontend_rules"`
	Qr                     QRouter           `json:"query_routing" toml:"query_routing" yaml:"query_routing"`
	FrontendTLS            *TLSConfig        `json:"frontend_tls" yaml:"frontend_tls" toml:"frontend_tls"`
	BackendRules           []*BackendRule    `json:"backend_rules" toml:"backend_rules" yaml:"backend_rules"`
	ShardMapping           map[string]*Shard `json:"shards" toml:"shards" yaml:"shards"`
	SchemaCacheBackendRule *BackendRule      `json:"schema_cache_backend_rule" toml:"schema_cache_backend_rule" yaml:"schema_cache_backend_rule"`

	WorkloadFile      string `json:"workload_file" toml:"workload_file" yaml:"workload_file"`
	WorkloadBatchSize int    `json:"workload_batch_size" toml:"workload_batch_size" yaml:"workload_batch_size"`

	ReusePort bool `json:"reuse_port" toml:"reuse_port" yaml:"reuse_port"`

	WithCoordinator bool `json:"with_coordinator" toml:"with_coordinator" yaml:"with_coordinator"`

	UseSystemdNotifier   bool `json:"use_systemd_notifier" toml:"use_systemd_notifier" yaml:"use_systemd_notifier"`
	SystemdNotifierDebug bool `json:"systemd_notifier_debug" toml:"systemd_notifier_debug" yaml:"systemd_notifier_debug"`
}

type QRouter struct {
	DefaultRouteBehaviour        DefaultRouteBehaviour `json:"default_route_behaviour" toml:"default_route_behaviour" yaml:"default_route_behaviour"`
	EnhancedMultiShardProcessing bool                  `json:"enhanced_multishard_processing" toml:"enhanced_multishard_processing" yaml:"enhanced_multishard_processing"`
}

type BackendRule struct {
	DB              string                     `json:"db" yaml:"db" toml:"db"`
	Usr             string                     `json:"usr" yaml:"usr" toml:"usr"`
	AuthRules       map[string]*AuthBackendCfg `json:"auth_rules" yaml:"auth_rules" toml:"auth_rules"`
	DefaultAuthRule *AuthBackendCfg            `json:"auth_rule" yaml:"auth_rule" toml:"auth_rule"`
	PoolDefault     bool                       `json:"pool_default" yaml:"pool_default" toml:"pool_default"`

	ConnectionLimit   int           `json:"connection_limit" yaml:"connection_limit" toml:"connection_limit"`
	ConnectionRetries int           `json:"connection_retries" yaml:"connection_retries" toml:"connection_retries"`
	ConnectionTimeout time.Duration `json:"connection_timeout" yaml:"connection_timeout" toml:"connection_timeout"`
	KeepAlive         time.Duration `json:"keep_alive" yaml:"keep_alive" toml:"keep_alive"`
	TcpUserTimeout    time.Duration `json:"tcp_user_timeout" yaml:"tcp_user_timeout" toml:"tcp_user_timeout"`
}

type FrontendRule struct {
	DB                    string   `json:"db" yaml:"db" toml:"db"`
	Usr                   string   `json:"usr" yaml:"usr" toml:"usr"`
	SearchPath            string   `json:"search_path" yaml:"search_path" toml:"search_path"`
	AuthRule              *AuthCfg `json:"auth_rule" yaml:"auth_rule" toml:"auth_rule"`
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
	RawHosts        []string `json:"hosts" toml:"hosts" yaml:"hosts"` // format host:port:availability_zone
	parsedHosts     []Host
	parsedAddresses []string
	once            sync.Once

	Type ShardType  `json:"type" toml:"type" yaml:"type"`
	TLS  *TLSConfig `json:"tls" yaml:"tls" toml:"tls"`
}

type Host struct {
	Address string // format host:port
	AZ      string // Availability zone
}

// parseHosts parses the raw hosts into a slice of Hosts.
// The format of the RawHost is host:port:availability_zone.
// If the availability_zone is not provided, it is empty.
// If the port is not provided, it does not matter
func (s *Shard) parseHosts() {
	for _, rawHost := range s.RawHosts {
		host := Host{}
		parts := strings.Split(rawHost, ":")
		if len(parts) > 3 {
			log.Printf("invalid host format: expected 'host:port:availability_zone', got '%s'", rawHost)
			continue
		} else if len(parts) == 3 {
			host.AZ = parts[2]
			host.Address = fmt.Sprintf("%s:%s", parts[0], parts[1])
		} else {
			host.Address = rawHost
		}

		s.parsedHosts = append(s.parsedHosts, host)
		s.parsedAddresses = append(s.parsedAddresses, host.Address)
	}
}

func (s *Shard) Hosts() []string {
	s.once.Do(s.parseHosts)

	return s.parsedAddresses
}

func (s *Shard) HostsAZ() []Host {
	s.once.Do(s.parseHosts)

	return s.parsedHosts
}

func ValueOrDefaultInt(value int, def int) int {
	if value == 0 {
		return def
	}
	return value
}

func ValueOrDefaultDuration(value time.Duration, def time.Duration) time.Duration {
	if value == 0 {
		return def
	}
	return value
}

// LoadRouterCfg loads the router configuration from the specified file path.
//
// Parameters:
//   - cfgPath (string): The path of the configuration file.
//
// Returns:
//   - error: An error if any occurred during the loading process.
func LoadRouterCfg(cfgPath string) (string, error) {
	var rcfg Router
	file, err := os.Open(cfgPath)
	if err != nil {
		cfgRouter = rcfg
		return "", err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("failed to close config file: %v", err)
		}
	}(file)

	if err := initRouterConfig(file, &rcfg); err != nil {
		cfgRouter = rcfg
		return "", err
	}

	if err := validateRouterConfig(&rcfg); err != nil {
		cfgRouter = rcfg
		return "", err
	}

	statistics.InitStatistics(rcfg.TimeQuantiles)

	configBytes, err := json.MarshalIndent(rcfg, "", "  ")
	if err != nil {
		cfgRouter = rcfg
		return "", err
	}

	// log.Println("Running config:", string(configBytes))
	cfgRouter = rcfg
	return string(configBytes), nil
}

// initRouterConfig initializes the router configuration from a file.
//
// Parameters:
// - file: *os.File - the file to read the configuration from.
// - cfgRouter: *Router - a pointer to the router configuration struct.
//
// Returns:
// - error: an error if the configuration file format is unknown or if there was an error decoding the file.
func initRouterConfig(file *os.File, cfgRouter *Router) error {
	if strings.HasSuffix(file.Name(), ".toml") {
		_, err := toml.NewDecoder(file).Decode(cfgRouter)
		return err
	}
	if strings.HasSuffix(file.Name(), ".yaml") {
		return yaml.NewDecoder(file).Decode(cfgRouter)
	}
	if strings.HasSuffix(file.Name(), ".json") {
		return json.NewDecoder(file).Decode(cfgRouter)
	}
	return fmt.Errorf("unknown config format type: %s. Use .toml, .yaml or .json suffix in filename", file.Name())
}

// validateRouterConfig checks the validity of the router configuration.
//
// Parameters:
//   - cfg (*Router): the router config to check
//
// Returns:
//   - string: JSON-formatted config
//   - error: an error if config is invaild, nil otherwise
func validateRouterConfig(cfg *Router) error {
	for sh, shCfg := range cfg.ShardMapping {
		if shCfg == nil {
			return spqrerror.Newf(spqrerror.SPQR_CONFIG_ERROR, "shard \"%s\" is stated in config, but no actual config is provided", sh)
		}
	}
	for _, rule := range cfg.BackendRules {
		for name, authRule := range rule.AuthRules {
			if authRule == nil {
				return spqrerror.Newf(spqrerror.SPQR_CONFIG_ERROR, "user \"%s\" is stated in backend rule \"%s.%s\", but no auth config is provided", name, rule.Usr, rule.DB)
			}
		}
	}
	for _, rule := range cfg.FrontendRules {
		if rule.AuthRule == nil {
			return spqrerror.Newf(spqrerror.SPQR_CONFIG_ERROR, "No auth rule provided for frontend rule \"%s.%s\"", rule.Usr, rule.DB)
		}
		switch rule.AuthRule.Method {
		case AuthGSS:
			if rule.AuthRule.GssConfig == nil {
				return spqrerror.Newf(spqrerror.SPQR_CONFIG_ERROR, "Frontend rule \"%s.%s\" has auth method GSS, but no GSS config is provided", rule.Usr, rule.DB)
			}
		case AuthLDAP:
			if rule.AuthRule.LDAPConfig == nil {
				return spqrerror.Newf(spqrerror.SPQR_CONFIG_ERROR, "Frontend rule \"%s.%s\" has auth method LDAP, but no LDAP config is provided", rule.Usr, rule.DB)
			}
		case AuthNotOK, AuthOK, AuthClearText, AuthMD5, AuthSCRAM:
			continue
		default:
			return spqrerror.Newf(spqrerror.SPQR_CONFIG_ERROR, "invalid auth method \"%s\"", rule.AuthRule.Method)
		}
	}
	return nil
}

// RouterConfig returns the router configuration.
//
// Parameters:
// - None.
//
// Returns:
// - *Router: a pointer to the router configuration struct.
func RouterConfig() *Router {
	return &cfgRouter
}

func GetHostOrHostname(host string) (string, error) {
	if host == "" {
		return os.Hostname()
	}
	return host, nil
}
