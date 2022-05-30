package config

import (
	"crypto/tls"
	"fmt"

	"github.com/wal-g/tracelog"
)

const (
	SSLMODEDISABLE = "disable"
	SSLMODEREQUIRE = "require"
)

type TLSConfig struct {
	SslMode  string `json:"sslmode" toml:"sslmode" yaml:"sslmode"`
	KeyFile  string `json:"key_file" toml:"key_file" yaml:"key_file"`
	CertFile string `json:"cert_file" toml:"cert_file" yaml:"cert_file"`
}

func (c *TLSConfig) IsTLSModeDisable() bool {
	return c == nil || c.SslMode == SSLMODEDISABLE || (c.CertFile == "" && c.KeyFile == "" || c.SslMode == "")
}

func (c *TLSConfig) Init() (*tls.Config, error) {
	tracelog.InfoLogger.Printf("Init TLS kek")
	if c.IsTLSModeDisable() {
		tracelog.InfoLogger.Printf("skip loading tls certs")
		return nil, nil
	}

	tracelog.InfoLogger.Printf("loading tls cert file %s, key file %s", c.CertFile, c.KeyFile)
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load tls conf: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}, nil
}
