package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/jackc/pgproto3/v2"
	"github.com/wal-g/tracelog"
	"net"
	"sync"
	"time"
)

var readResp = flag.Bool("v", false, "Logs every packet in great detail")
var doErr = flag.Bool("e", false, "Logs every packet in great detail")

func getC() (net.Conn, error) {
	const proto = "tcp"
	const addr = "[::1]:6432"
	return net.Dial(proto, addr)
}

func readCnt(fr *pgproto3.Frontend, count int) error {
	for i := 0; i < count; i++ {
		if msg, err := fr.Receive(); err != nil {
			return err
		} else {
			tracelog.InfoLogger.Printf("received %T msg", msg)
		}
	}

	return nil
}

var okerr = errors.New("something")

func waitRFQ(fr *pgproto3.Frontend) error {
	for {
		if msg, err := fr.Receive(); err != nil {
			return err
		} else {
			tracelog.InfoLogger.Printf("received %+v msg", msg)
			switch msg.(type) {
			case *pgproto3.ErrorResponse:
				return okerr
			case *pgproto3.ReadyForQuery:
				return nil
			}
		}
	}
}

func prepLong(fr *pgproto3.Frontend, waitforres bool) error {
	if *doErr {
		if err := fr.Send(&pgproto3.Parse{
			Name:  "s1",
			Query: fmt.Sprintf("SELECT 1/0"),
		}); err != nil {
			return err
		}
	} else {
		if err := fr.Send(&pgproto3.Parse{
			Name:  "s1",
			Query: fmt.Sprintf("SELECT 1"),
		}); err != nil {
			return err
		}
	}
	if err := fr.Send(&pgproto3.Describe{
		Name:       "s1",
		ObjectType: byte('S'),
	}); err != nil {
		return err
	}

	if err := fr.Send(&pgproto3.Sync{}); err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("reading prep parse")
	if err := waitRFQ(fr); err != nil {
		return err
	}

	if err := fr.Send(&pgproto3.Bind{
		PreparedStatement: "s1",
	}); err != nil {
		return err
	}

	if err := fr.Send(&pgproto3.Execute{}); err != nil {
		return err
	}

	if err := fr.Send(&pgproto3.Sync{}); err != nil {
		return err
	}

	if !waitforres {
		tracelog.InfoLogger.Printf("not reading prep resp")
		return nil
	}

	tracelog.InfoLogger.Printf("reading prep resp")
	return waitRFQ(fr)
}

func gaogao(wg *sync.WaitGroup, waitforres bool) {
	defer wg.Done()

	conn, err := getC()
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to get conn %w", err)
		if err != okerr {
			panic(err)
		}
		return
	}
	defer conn.Close()

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)

	if err := frontend.Send(&pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters: map[string]string{
			"user":     "user1",
			"database": "db1",
			"password": "12345678",
		},
	}); err != nil {
		tracelog.ErrorLogger.Printf("startup failed %w", err)
		if err != okerr {
			panic(err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	if err := waitRFQ(frontend); err != nil {
		tracelog.ErrorLogger.Printf("startup failed %w", err)
		if err != okerr {
			panic(err)
		}
		return
	}

	if err := prepLong(frontend, waitforres); err != nil {
		tracelog.ErrorLogger.Printf("prep failed %w", err)
		if err != okerr {
			panic(err)
		}
		return
	}

	tracelog.InfoLogger.Printf("ok")
}

func main() {
	flag.Parse()

	wg := sync.WaitGroup{}
	cnt := 1
	wg.Add(cnt)
	for i := 0; i < cnt; i++ {
		go gaogao(&wg, *readResp)
	}

	wg.Wait()
}
