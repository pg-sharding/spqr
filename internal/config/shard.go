package config

import "crypto/tls"

type ShardCfg struct {
	ConnAddr string `json:"conn_addr" toml:"conn_addr" yaml:"conn_addr"`
	ConnDB   string `json:"conn_db" toml:"conn_db" yaml:"conn_db"`
	ConnUsr  string `json:"conn_usr" toml:"conn_usr" yaml:"conn_usr"`

	Passwd string `json:"passwd" toml:"passwd" yaml:"passwd"`

	TLSCfg TLSConfig `json:"tls" yaml:"tls" toml:"tls"`

	TLSConfig *tls.Config
}

func (s *ShardCfg) Init(cfg *tls.Config) error {
	s.TLSConfig = cfg
	return nil
}
