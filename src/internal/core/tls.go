package core

type TLSConfig struct {
	CAPath      string      `json:"ca_path" toml:"ca_path" yaml:"ca_path"`
	ServPath    string      `json:"serv_key_path" toml:"serv_key_path" yaml:"serv_key_path"`
	TLSSertPath string      `json:"tls_cert_path" toml:"tls_cert_path" yaml:"tls_cert_path"`
}
