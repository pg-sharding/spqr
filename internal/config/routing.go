package config

type RouteKeyCfg struct {
	Usr string `json:"usr" yaml:"usr" toml:"usr"`
	DB  string `json:"db" yaml:"db" toml:"db"`
}

type RouterConfig struct {
	BackendRules  []*BERule `json:"backend_rules" toml:"backend_rules" yaml:"backend_rules"`
	FrontendRules []*FRRule `json:"frontend_rules" toml:"frontend_rules" yaml:"frontend_rules"`

	MaxConnPerRoute int `json:"max_conn_per_route" toml:"max_conn_per_route" yaml:"max_conn_per_route"`

	PROTO string `json:"proto" toml:"proto" yaml:"proto"`

	// listen cfg
	TLSCfg TLSConfig `json:"tls" yaml:"tls" toml:"tls"`

	// shards
	ShardMapping map[string]ShardCfg `json:"shard_mapping" toml:"shard_mapping" yaml:"shard_mapping"`
}

type BERule struct {
	RK RouteKeyCfg `json:"route_key_cfg" yaml:"route_key_cfg" toml:"route_key_cfg"`

	PoolDiscard  bool `json:"pool_discard" yaml:"pool_discard" toml:"pool_discard"`
	PoolRollback bool `json:"pool_rollback" yaml:"pool_rollback" toml:"pool_rollback"`
}

type FRRule struct {
	RK RouteKeyCfg `json:"route_key_cfg" yaml:"route_key_cfg" toml:"route_key_cfg"`

	ClientMax int `json:"client_max" yaml:"client_max" toml:"client_max"`

	PoolingMode PoolingMode `json:"pooling_mode" yaml:"pooling_mode" toml:"pooling_mode"`

	// TODO: validate!
	AuthRule AuthRule `json:"auth_rule" yaml:"auth_rule" toml:"auth_rule"`
}
