package config

import (
	"time"
)

var cfgCoordinator Coordinator

type Coordinator struct {
	LogFileName   string `json:"log_filename" toml:"log_filename" yaml:"log_filename"`
	LogLevel      string `json:"log_level" toml:"log_level" yaml:"log_level"`
	PrettyLogging bool   `json:"pretty_logging" toml:"pretty_logging" yaml:"pretty_logging"`
	// QdbAddr is deprecated, use QdbAddrs instead
	QdbAddr                   string          `json:"qdb_addr" toml:"qdb_addr" yaml:"qdb_addr"`
	QdbAddrs                  []string        `json:"qdb_addrs" toml:"qdb_addrs" yaml:"qdb_addrs"`
	CoordinatorPort           string          `json:"coordinator_port" toml:"coordinator_port" yaml:"coordinator_port"`
	GrpcApiPort               string          `json:"grpc_api_port" toml:"grpc_api_port" yaml:"grpc_api_port"`
	Host                      string          `json:"host" toml:"host" yaml:"host"`
	FrontendTLS               *TLSConfig      `json:"frontend_tls" yaml:"frontend_tls" toml:"frontend_tls"`
	FrontendRules             []*FrontendRule `json:"frontend_rules" toml:"frontend_rules" yaml:"frontend_rules"`
	ShardDataCfg              string          `json:"shard_data" toml:"shard_data" yaml:"shard_data"`
	UseSystemdNotifier        bool            `json:"use_systemd_notifier" toml:"use_systemd_notifier" yaml:"use_systemd_notifier"`
	SystemdNotifierDebug      bool            `json:"systemd_notifier_debug" toml:"systemd_notifier_debug" yaml:"systemd_notifier_debug"`
	IterationTimeout          time.Duration   `json:"iteration_timeout" toml:"iteration_timeout" yaml:"iteration_timeout"`
	LockIterationTimeout      time.Duration   `json:"lock_iteration_timeout" toml:"lock_iteration_timeout" yaml:"lock_iteration_timeout"`
	EnableRoleSystem          bool            `json:"enable_role_system" toml:"enable_role_system" yaml:"enable_role_system"`
	RolesFile                 string          `json:"roles_file" toml:"roles_file" yaml:"roles_file"`
	ManageShardsByCoordinator bool            `json:"manage_shards_by_coordinator" yaml:"manage_shards_by_coordinator" toml:"manage_shards_by_coordinator"`

	EtcdMaxSendBytes          int    `json:"etcd_max_send_bytes" toml:"etcd_max_send_bytes" yaml:"etcd_max_send_bytes"`
	EtcdMaxTxnOps             int    `json:"etcd_max_txn_ops" toml:"etcd_max_txn_ops" yaml:"etcd_max_txn_ops"`
	DataMoveDisableTriggers   bool   `json:"data_move_disable_triggers" toml:"data_move_disable_triggers" yaml:"data_move_disable_triggers"`
	DataMoveBoundBatchSize    int64  `json:"data_move_bound_batch_size" toml:"data_move_bound_batch_size" yaml:"data_move_bound_batch_size"`
	DataMoveQueryLogLevel     string `json:"data_move_query_log_level" toml:"data_move_query_log_level" yaml:"data_move_query_log_level"`
	DataMoveAwaitPIDException string `json:"data_move_await_pid_exception" toml:"data_move_await_pid_exception" yaml:"data_move_await_pid_exception"`

	ForbidDirectShardQueries bool `json:"forbid_direct_shard_queries" toml:"forbid_direct_shard_queries" yaml:"forbid_direct_shard_queries"`

	// gRPC keepalive settings for router connections
	// Prevents connections from being closed by network intermediaries during idle periods
	RouterKeepaliveTime    time.Duration `json:"router_keepalive_time" toml:"router_keepalive_time" yaml:"router_keepalive_time"`
	RouterKeepaliveTimeout time.Duration `json:"router_keepalive_timeout" toml:"router_keepalive_timeout" yaml:"router_keepalive_timeout"`

	EnableICP bool `json:"enable_icp" toml:"enable_icp" yaml:"enable_icp"`
}

var _ Config = &Coordinator{}

func (c *Coordinator) ApplyDefaults() {
	c.DataMoveBoundBatchSize = 10_000
	c.DataMoveQueryLogLevel = "debug"
	c.DataMoveAwaitPIDException = "true"
}

func (c *Coordinator) PostProcess() error {
	if c.QdbAddr != "" && c.QdbAddrs == nil {
		c.QdbAddrs = []string{c.QdbAddr}
	}

	return nil
}

// LoadCoordinatorCfg loads the coordinator configuration from the specified file path.
//
// Parameters:
//   - cfgPath (string): The path of the configuration file.
//
// Returns:
//   - string: JSON-formatted config
//   - error: An error if any occurred during the loading process.
func LoadCoordinatorCfg(cfgPath string) (string, error) {
	c := &Coordinator{}
	configStr, err := LoadConfig(cfgPath, c)
	if err != nil {
		return "", err
	}

	cfgCoordinator = *c
	return configStr, nil
}

// CoordinatorConfig returns a pointer to the Coordinator configuration.
//
// Returns:
//   - *Coordinator: a pointer to the Coordinator configuration.
func CoordinatorConfig() *Coordinator {
	return &cfgCoordinator
}
