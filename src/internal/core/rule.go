package core

type BERule struct {
	TLSCfg TLSConfig `json:"tls" yaml:"tls" toml:"tls"`

	SHStorage Storage `json:"storage_cfg" yaml:"storage_cfg" toml:"storage_cfg"`
}

type FRRule struct {
	Usr       string `json:"usr" yaml:"usr" toml:"usr"`
	DB        string `json:"db" yaml:"db" toml:"db"`
	ClientMax int    `json:"client_max" yaml:"client_max" toml:"client_max"`

	AuthRule AuthRule `json:"auth_rule" yaml:"auth_rule" toml:"auth_rule"`
}
