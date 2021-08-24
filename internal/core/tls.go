package core

const sslproto = 80877103

type TLSConfig struct {
	KeyFile    string `json:"key_file" toml:"key_file" yaml:"key_file"`
	CertFile string `json:"cert_file" toml:"cert_file" yaml:"cert_file"`
}
