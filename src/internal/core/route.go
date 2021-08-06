package core

import (
	"github.com/jackc/pgproto3"
	"net"
)

type routeKey struct {
	usr string
	db string
}


type shardKey struct {
	i int
}

type Route struct {
	rule *Rule

	servPoolPending map[shardKey]*ShServer

	client *ShClient

	shardConn *ShServer
	//serv   ShServer
}

func (r *Route) assignCLient(client * ShClient) {
	r.client = client
}

func (r *Route) Client() *ShClient {
	return r.client
}

func (r *Route) ShardConn() *ShServer {
	return r.shardConn
}

func (r*Route) Unroute() {
	r.shardConn = nil
}

func NewRoute(rule *Rule) *Route {
	return &Route{
		rule: rule,
		servPoolPending: map[shardKey]*ShServer{

		},
	}
}

func (r*Route) ProcQuery(query *pgproto3.Query) (byte, error) {

	if err := r.shardConn.Send(query); err != nil {
		return 0, err
	}

	for {
		msg, err := r.shardConn.Receive()
		if err != nil {
			return 0, err
		}
		switch v := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return v.TxStatus, nil
		}

		err = r.client.Send(msg)
		if err != nil {
			//tracelog.InfoLogger.Println(reflect.TypeOf(msg))
			//tracelog.InfoLogger.Println(msg)
			return 0, err
		}
	}
}



func (r *Route) smFromSh() *pgproto3.StartupMessage {

	sm := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"application_name": "shgo",
			"client_encoding":  "UTF8",
			"user":             r.rule.Usr,
			"database":         r.rule.DB,
		},
	}
	return sm
}

func (r *Route) PushConn(netconn net.Conn) error {

	srv := NewServer(r.rule, netconn)
	if r.rule.SHStorage.ReqSSL {
		if err := srv.ReqBackendSsl(); err != nil {
			return err
		}
	}

	return srv.initConn(r.smFromSh())
}