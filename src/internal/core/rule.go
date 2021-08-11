package core

import "github.com/shgo/src/internal/conn"

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

	PoolingMode conn.PoolingMode `json:"pooling_mode" yaml:"pooling_mode" toml:"pooling_mode"`

	// TODO: validate!
	AuthRule AuthRule `json:"auth_rule" yaml:"auth_rule" toml:"auth_rule"`
}
