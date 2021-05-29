package conn

import (
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"reflect"
	//"reflect"
	"sync"

	"github.com/jackc/pgproto3"
	"github.com/shgo/src/util"
	//"github.com/wal-g///tracelog"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
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

type ShardConnCfg struct {
	ConnAddr string `json:"conn_addr" toml:"conn_addr" yaml:"conn_addr"`
	ConnDB   string `json:"conn_db" toml:"conn_db" yaml:"conn_db"`
	ConnUsr  string `json:"conn_usr" toml:"conn_usr" yaml:"conn_usr"`
	Passwd   string `json:"passwd" toml:"passwd" yaml:"passwd"`
	ReqSSL   bool   `json:"require_ssl" toml:"require_ssl" yaml:"require_ssl"`
}

type Config struct {
	ShardMapping    []ShardConnCfg `json:"shard_mapping" toml:"shard_mapping" yaml:"shard_mapping"`
	MaxConnPerRoute int            `json:"max_conn_per_route" toml:"max_conn_per_route" yaml:"max_conn_per_route"`
	CAPath          string         `json:"ca_path" toml:"ca_path" yaml:"ca_path"`
	ServPath        string         `json:"serv_key_path" toml:"serv_key_path" yaml:"serv_key_path"`
	TLSSertPath     string         `json:"tls_cert_path" toml:"tls_cert_path" yaml:"tls_cert_path"`
	ReqSSL          bool           `json:"require_ssl" toml:"require_ssl" yaml:"require_ssl"`
}

type Connector struct {
	cfg Config

	connMp [][]*pgproto3.Frontend
	connMu []sync.Mutex
}

func (conn *Connector) initConn(shardConn *pgproto3.Frontend, sm pgproto3.StartupMessage,
	shardConnCfg ShardConnCfg) error {
	err := shardConn.Send(&sm)
	if err != nil {
		util.Fatal(err)
		return err
	}

	for {
		//tracelog.InfoLogger.Println("round inner")
		msg, err := shardConn.Receive()
		if err != nil {
			util.Fatal(err)
			return err
		}
		tracelog.InfoLogger.Println(reflect.TypeOf(msg))
		tracelog.InfoLogger.Println(msg)
		//fatal(backend.Send(msg))
		switch v := msg.(type) {
		case *pgproto3.ReadyForQuery:
			//tracelog.InfoLogger.Println("inner ok")
			return nil

			//!! backend auth

			//
		case *pgproto3.Authentication:

			fmt.Printf("Auth type supp %T\n", v)

			switch v.Type {
			case pgproto3.AuthTypeOk:
				continue
			case pgproto3.AuthTypeMD5Password:
				hash := md5.New()

				hash.Write([]byte(shardConnCfg.Passwd + shardConnCfg.ConnUsr))

				res := hash.Sum(nil)

				res = append(res, v.Salt[0], v.Salt[1], v.Salt[2], v.Salt[3])
				tracelog.InfoLogger.Println("auth bypass md5 %s", res)

				str1 := hex.EncodeToString(res)

				hash2 := md5.New()
				hash2.Write([]byte(str1))

				res2 := hash2.Sum(nil)

				psswd := hex.EncodeToString(res2)

				tracelog.InfoLogger.Println("auth bypass md5 %s", psswd)

				shardConn.Send(&pgproto3.PasswordMessage{Password: "md5" + psswd})

			case pgproto3.AuthTypeCleartextPassword:
				tracelog.InfoLogger.Println("auth bypass %s", shardConnCfg.Passwd)
				shardConn.Send(&pgproto3.PasswordMessage{Password: shardConnCfg.Passwd})
			default:
				return xerrors.Errorf("auth type %T not supportes", v.Type)

			}
		}
	}
}

func (conn *Connector) smFromSh(
	shardConn ShardConnCfg,
) pgproto3.StartupMessage {

	sm := pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"application_name": "shgo",
			"client_encoding":  "UTF8",
			"user":             shardConn.ConnUsr,
			"database":         shardConn.ConnDB,
		},
	}
	return sm
}

const sslproto = 80877103

func (conn *Connector) reqBackendSsl(pgconn net.Conn) (*tls.Conn, error) {

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 8)
	b = append(b, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(b[4:], sslproto)

	_, err := pgconn.Write(b)

	if err != nil {
		panic(err)
	}

	resp := make([]byte, 1)

	pgconn.Read(resp)
	fmt.Printf("%v", resp)

	sym := resp[0]

	fmt.Printf("%v\n", sym)

	if sym != 'S' {
		panic("SSL SHOUD BE ENABLED")
	}

	cert, err := tls.LoadX509KeyPair(conn.cfg.TLSSertPath, conn.cfg.ServPath)
	if err != nil {
		panic(err)
	}

	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}

	return tls.Client(pgconn, cfg), err
}

func (conn *Connector) Connect(i int) (*pgproto3.Frontend, error) {
	var fr *pgproto3.Frontend

	shConf := conn.cfg.ShardMapping[i]

	conn.connMu[i].Lock()
	{
		if connl := conn.connMp[i]; len(connl) > 0 {
			fr = connl[0]
			connl = connl[1:]
			conn.connMp[i] = connl
		} else {

			pgConn, err := net.Dial("tcp6", conn.cfg.ShardMapping[i].ConnAddr)
			util.Fatal(err)

			if shConf.ReqSSL {
				pgConn, err = conn.reqBackendSsl(pgConn)
			}

			if err != nil {
				return nil, err
			}

			fr, err = pgproto3.NewFrontend(pgproto3.NewChunkReader(pgConn), pgConn)
			util.Fatal(err)

			if err := conn.initConn(fr, conn.smFromSh(shConf), shConf); err != nil {
				return nil, err
			}
		}
	}
	conn.connMu[i].Unlock()
	return fr, nil
}

func (conn *Connector) ProcQuery(b *pgproto3.Backend, query *pgproto3.Query, c *pgproto3.Frontend) (byte, error) {

	if err := c.Send(query); err != nil {
		return 0, err
	}

	for {
		msg, err := c.Receive()
		if err != nil {
			return 0, err
		}
		switch v := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return v.TxStatus, nil
		}

		err = b.Send(msg)
		if err != nil {

			//tracelog.InfoLogger.Println(reflect.TypeOf(msg))
			//tracelog.InfoLogger.Println(msg)
			return 0, err
		}
	}
}

func (conn *Connector) ListShards() []string {
	var ret []string

	for _, sh := range conn.cfg.ShardMapping {
		ret = append(ret, sh.ConnAddr)
	}

	return ret
}

func (conn *Connector) ReleaseConn(shindx int, pgconn *pgproto3.Frontend) {

	conn.connMu[shindx].Lock()
	conn.connMp[shindx] = append(
		conn.connMp[shindx],
		pgconn,
	)
	conn.connMu[shindx].Unlock()
}

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
