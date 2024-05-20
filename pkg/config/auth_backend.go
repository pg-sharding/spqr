package config

type AuthBackendCfg struct {
	Password string `json:"password" yaml:"password" toml:"password"`
	Usr      string `json:"usr" yaml:"usr" toml:"usr"`
}
