package core

import (
	"github.com/jackc/pgproto3"
	"github.com/shgo/src/internal/conn"
	"github.com/shgo/src/util"
	"net"
)

type ShClient struct {
	rule *Rule
	fr pgproto3.Frontend
}

func (cl *ShClient) Send(msg pgproto3.FrontendMessage) error {
	return cl.fr.Send(msg)
}


func NewClient(pgconn *net.Conn) *ShClient {


	ret := ShClient{

	}
	//
	//pgConn, err := net.Dial("tcp6", conn.cfg.ShardMapping[i].ConnAddr)
	//util.Fatal(err)

	if ret.rule.SHStorage.ReqSSL {
		pgConn, err = conn.ReqBackendSsl(pgConn)
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