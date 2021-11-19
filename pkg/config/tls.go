package config

import (
	"crypto/tls"

	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
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

func InitTLS(sslMode, certFile, keyFile string) (*tls.Config, error) {
	if sslMode != SSLMODEDISABLE {
		tracelog.InfoLogger.Printf("loading tls cert file %s, key file %s", certFile, keyFile)
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, xerrors.Errorf("failed to load tls conf: %w", err)
		}
		return &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}, nil
	} else {
		tracelog.InfoLogger.Printf("skip loading tls certs")
	}
	return nil, nil
}
