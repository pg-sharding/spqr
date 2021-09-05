package config

type SpqrConfig struct {
	Addr    string `json:"addr" toml:"addr" yaml:"addr"`
	ADMAddr string `json:"adm_addr" toml:"adm_addr" yaml:"adm_addr"`
	PROTO   string `json:"proto" toml:"proto" yaml:"proto"`

	RouterCfg RouterConfig `json:"router" toml:"router" yaml:"router"`

	HttpConfig HttpConf `json:"http_conf" toml:"http_conf" yaml:"http_conf"`

	ExecuterCfg ExecuterCfg `json:"executer" toml:"executer" yaml:"executer"`
}

type HttpConf struct {
	Addr string `json:"http_addr" toml:"http_addr" yaml:"http_addr"`
}

const (
	SSLMODEDISABLE = "disable"
	SSLMODEREQUIRE = "require"
)

type TLSConfig struct {
	SslMode  string `json:"sslmode" toml:"sslmode" yaml:"sslmode"`
	KeyFile  string `json:"key_file" toml:"key_file" yaml:"key_file"`
	CertFile string `json:"cert_file" toml:"cert_file" yaml:"cert_file"`
}
