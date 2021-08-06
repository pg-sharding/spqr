package conn

import (
	"github.com/shgo/src/internal/core"
	"net"
	//"reflect"
	"sync"

	"github.com/jackc/pgproto3"
)

const (
	SERVER_ACTIVE  = "ACTIVE"
	SERVER_PENDING = "PENDING"
)

type SERVER_STATE string

type shardConnKey struct {
	shid  int
	state SERVER_STATE
}

type server struct {
	conn  *pgproto3.Frontend
	state SERVER_STATE
}

type servPool struct {
	shardConns []*pgproto3.Frontend
}

type PgConn struct {
	net.Conn
}

type Config struct {
	ShardMapping    []core.Rule    `json:"shard_mapping" toml:"shard_mapping" yaml:"shard_mapping"`
	Storages        []core.Storage
	MaxConnPerRoute int            `json:"max_conn_per_route" toml:"max_conn_per_route" yaml:"max_conn_per_route"`
	CAPath          string         `json:"ca_path" toml:"ca_path" yaml:"ca_path"`
	ServPath        string         `json:"serv_key_path" toml:"serv_key_path" yaml:"serv_key_path"`
	TLSSertPath     string         `json:"tls_cert_path" toml:"tls_cert_path" yaml:"tls_cert_path"`
	ReqSSL          bool           `json:"require_ssl" toml:"require_ssl" yaml:"require_ssl"`

	PROTO          string  `json:"proto" toml:"proto" yaml:"proto"`

}

type Connector struct {
	cfg Config

	router core.Router

	connMp [][]*pgproto3.Frontend
	connMu []sync.Mutex
}


const sslproto = 80877103


func (conn *Connector) Connect(i int) (net.Conn, error) {

	shConf := conn.cfg.ShardMapping[i]

	return net.Dial(conn.cfg.PROTO, shConf.SHStorage.ConnAddr)

	//conn.connMu[i].Lock()
	//{
	//	if connl := conn.connMp[i]; len(connl) > 0 {
	//		serv := connl[0]
	//		connl = connl[1:]
	//		conn.connMp[i] = connl
	//	} else {
	//
	//
	//
	//		serv := core.NewServer(
	//			)
	//	}
	//}
	//conn.connMu[i].Unlock()
	//return serv, nil
}

func (conn *Connector) ListShards() []string {
	var ret []string

	for _, sh := range conn.cfg.ShardMapping {
		ret = append(ret, sh.SHStorage.ConnAddr)
	}

	return ret
}
//
//func (conn *Connector) ReleaseConn(shindx int, serv *core.ShServer) {
//
//	conn.connMu[shindx].Lock()
//	conn.connMp[shindx] = append(
//		conn.connMp[shindx],
//		pgconn,
//	)
//	conn.connMu[shindx].Unlock()
//}

func NewConnector(cfg Config) Connector {
	conn := Connector{
		cfg:    cfg,
		connMp: [][]*pgproto3.Frontend{},
		connMu: []sync.Mutex{},
	}

	for range cfg.ShardMapping {
		conn.connMu = append(conn.connMu, sync.Mutex{})

		conn.connMp = append(conn.connMp, []*pgproto3.Frontend{})
	}

	return conn
}
