package config

type RouterConfig struct {
	BackendRules    []*BERule `json:"backend_rules" toml:"backend_rules" yaml:"backend_rules"`
	FrontendRules   []*FRRule `json:"frontend_rules" toml:"frontend_rules" yaml:"frontend_rules"`
	MaxConnPerRoute int       `json:"max_conn_per_route" toml:"max_conn_per_route" yaml:"max_conn_per_route"`
	ReqSSL          bool      `json:"require_ssl" toml:"require_ssl" yaml:"require_ssl"`
	TLSCfg          TLSConfig `json:"tls" yaml:"tls" toml:"tls"`
	PROTO           string    `json:"proto" toml:"proto" yaml:"proto"`
}
type BERule struct {
	TLSCfg TLSConfig `json:"tls" yaml:"tls" toml:"tls"`

	PoolDiscard  bool `json:"pool_discard" yaml:"pool_discard" toml:"pool_discard"`
	PoolRollback bool `json:"pool_rollback" yaml:"pool_rollback" toml:"pool_rollback"`

	SHStorage Storage `json:"storage_cfg" yaml:"storage_cfg" toml:"storage_cfg"`
}

type FRRule struct {
	Usr       string `json:"usr" yaml:"usr" toml:"usr"`
	DB        string `json:"db" yaml:"db" toml:"db"`
	ClientMax int    `json:"client_max" yaml:"client_max" toml:"client_max"`

	PoolingMode PoolingMode `json:"pooling_mode" yaml:"pooling_mode" toml:"pooling_mode"`

	// TODO: validate!
	AuthRule AuthRule `json:"auth_rule" yaml:"auth_rule" toml:"auth_rule"`
}
