package config

import (
	"crypto/tls"

	"golang.org/x/xerrors"
)

type InstanceCFG struct {
	ConnAddr string `json:"conn_addr" toml:"conn_addr" yaml:"conn_addr"`
	Proto    string `json:"proto" toml:"proto" yaml:"proto"`
}

type ShardType string

const (
	DataShard  = ShardType("data")
	WorldShard = ShardType("world")
)

type ShardCfg struct {
	Hosts []*InstanceCFG `json:"hosts" toml:"hosts" yaml:"hosts"`

	ConnDB  string `json:"conn_db" toml:"conn_db" yaml:"conn_db"`
	ConnUsr string `json:"conn_usr" toml:"conn_usr" yaml:"conn_usr"`

	Passwd string    `json:"passwd" toml:"passwd" yaml:"passwd"`
	ShType ShardType `json:"shard_type" toml:"shard_type" yaml:"shard_type"`

	TLSCfg TLSConfig `json:"tls" yaml:"tls" toml:"tls"`

	TLSConfig *tls.Config
}

func (sh *ShardCfg) InitShardTLS() error {

	shardTLSConfig, err := InitTLS(sh.TLSCfg.SslMode, sh.TLSCfg.CertFile, sh.TLSCfg.KeyFile)
	if err != nil {
		return xerrors.Errorf("init shard TLS: %w", err)
	}
	sh.TLSConfig = shardTLSConfig

	return nil
}
