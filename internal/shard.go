package internal

import (
	"crypto/tls"
	"encoding/binary"
	"log"
	"net"

	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pkg/errors"
)

type Shard interface {
	Connect(proto string) (net.Conn, error)
	ReqBackendSsl(srv *ServerImpl) error
	Cfg() *config.ShardCfg
	Name() string
}

type ShardImpl struct {
	cfg *config.ShardCfg

	lg log.Logger

	name string
}

func (sh *ShardImpl) Name() string {
	return sh.name
}

func (sh *ShardImpl) Cfg() *config.ShardCfg {
	return sh.cfg
}

func (sh *ShardImpl) Connect(proto string) (net.Conn, error) {
	return net.Dial(proto, sh.cfg.ConnAddr)
}

var _ Shard = &ShardImpl{}

func NewShard(name string, cfg *config.ShardCfg) Shard {
	return &ShardImpl{
		cfg:  cfg,
		name: name,
	}
}

func (sh *ShardImpl) ReqBackendSsl(srv *ServerImpl) error {
	if !sh.cfg.TLSCfg.ReqSSL {
		return nil
	}

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 8)
	// Gen salt
	b = append(b, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(b[4:], sslproto)

	_, err := srv.conn.Write(b)

	if err != nil {
		return errors.Wrap(err, "ReqBackendSsl")
	}

	resp := make([]byte, 1)

	if _, err := srv.conn.Read(resp); err != nil {
		return err
	}

	sym := resp[0]

	if sym != 'S' {
		return errors.New("SSL should be enabled")
	}

	srv.conn = tls.Client(srv.conn, sh.cfg.TLSConfig)
	return nil
}
