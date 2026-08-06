package config

type AuthBackendCfg struct {
	Password string `json:"password" yaml:"password" toml:"password" secret:"true"`
	Usr      string `json:"usr" yaml:"usr" toml:"usr"`
}
