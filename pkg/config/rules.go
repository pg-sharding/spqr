package config

import "time"

type BackendRule struct {
	DB              string                     `json:"db" yaml:"db" toml:"db"`
	Usr             string                     `json:"usr" yaml:"usr" toml:"usr"`
	AuthRules       map[string]*AuthBackendCfg `json:"auth_rules" yaml:"auth_rules" toml:"auth_rules"`
	DefaultAuthRule *AuthBackendCfg            `json:"auth_rule" yaml:"auth_rule" toml:"auth_rule"`
	PoolDefault     bool                       `json:"pool_default" yaml:"pool_default" toml:"pool_default"`

	ConnectionLimit    int           `json:"connection_limit" yaml:"connection_limit" toml:"connection_limit"`
	ConnectionRetries  int           `json:"connection_retries" yaml:"connection_retries" toml:"connection_retries"`
	ConnectionTimeout  time.Duration `json:"connection_timeout" yaml:"connection_timeout" toml:"connection_timeout"`
	KeepAlive          time.Duration `json:"keep_alive" yaml:"keep_alive" toml:"keep_alive"`
	TcpUserTimeout     time.Duration `json:"tcp_user_timeout" yaml:"tcp_user_timeout" toml:"tcp_user_timeout"`
	PulseCheckInterval time.Duration `json:"pulse_check_interval" yaml:"pulse_check_interval" toml:"pulse_check_interval"`
}

type FrontendRule struct {
	DB       string   `json:"db" yaml:"db" toml:"db"`
	Usr      string   `json:"usr" yaml:"usr" toml:"usr"`
	AuthRule *AuthCfg `json:"auth_rule" yaml:"auth_rule" toml:"auth_rule"`

	// Pool settings and search_path does not take effect for coordinator
	SearchPath            string   `json:"search_path" yaml:"search_path" toml:"search_path"`
	PoolMode              PoolMode `json:"pool_mode" yaml:"pool_mode" toml:"pool_mode"`
	PoolDiscard           bool     `json:"pool_discard" yaml:"pool_discard" toml:"pool_discard"`
	PoolRollback          bool     `json:"pool_rollback" yaml:"pool_rollback" toml:"pool_rollback"`
	PoolPreparedStatement bool     `json:"pool_prepared_statement" yaml:"pool_prepared_statement" toml:"pool_prepared_statement"`
	PoolDefault           bool     `json:"pool_default" yaml:"pool_default" toml:"pool_default"`
}
