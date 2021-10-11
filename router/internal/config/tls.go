package config

const (
	SSLMODEDISABLE = "disable"
	SSLMODEREQUIRE = "require"
)

type TLSConfig struct {
	SslMode  string `json:"sslmode" toml:"sslmode" yaml:"sslmode"`
	KeyFile  string `json:"key_file" toml:"key_file" yaml:"key_file"`
	CertFile string `json:"cert_file" toml:"cert_file" yaml:"cert_file"`
}
