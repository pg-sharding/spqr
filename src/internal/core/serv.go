package core

import (
	"github.com/jackc/pgproto3"
	"github.com/shgo/src/util"
	"github.com/wal-g/tracelog"
	"reflect"
)

type ShServer struct {
	rule *Rule
	fr *pgproto3.Frontend
}

func (serv *ShServer) initConn(shardConn *pgproto3.Frontend, sm *pgproto3.StartupMessage) error {
	err := shardConn.Send(sm)
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

			//!! backend authBackend
		case *pgproto3.Authentication:
			err := authBackend(serv, v, shardConnCfg)
			if err != nil {
				return err
			}
		}
	}
}
