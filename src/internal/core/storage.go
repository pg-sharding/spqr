package core

import "crypto/tls"

type Storage struct {
	ConnAddr string `json:"conn_addr" toml:"conn_addr" yaml:"conn_addr"`
	ConnDB   string `json:"conn_db" toml:"conn_db" yaml:"conn_db"`
	ConnUsr  string `json:"conn_usr" toml:"conn_usr" yaml:"conn_usr"`
	Passwd   string `json:"passwd" toml:"passwd" yaml:"passwd"`
	ReqSSL   bool   `json:"require_ssl" toml:"require_ssl" yaml:"require_ssl"`

	cfg *tls.Config
}

func (s *Storage) Init(cfg *tls.Config) error {
	s.cfg = cfg
	return nil
}
