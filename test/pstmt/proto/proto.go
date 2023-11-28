package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

var dodesc = flag.Bool("d", false, "lol")

func getC() (net.Conn, error) {
	const proto = "tcp"
	const addr = "[::1]:6432"
	return net.Dial(proto, addr)
}

var okerr = errors.New("something")

// nolint
func readCnt(fr *pgproto3.Frontend, count int) error {
	for i := 0; i < count; i++ {
		if _, err := fr.Receive(); err != nil {
			return err
		}
	}

	return nil
}

func waitRFQ(fr *pgproto3.Frontend) error {
	for {
		if msg, err := fr.Receive(); err != nil {
			return err
		} else {
			switch msg.(type) {
			case *pgproto3.ErrorResponse:
				return okerr
			case *pgproto3.ReadyForQuery:
				return nil
			}
		}
	}
}

func prepBase(fr *pgproto3.Frontend) error {

	fr.Send(&pgproto3.Parse{
		Name:  "s1",
		Query: "show transaction_read_only",
	})
	if *dodesc {
		fr.Send(&pgproto3.Describe{
			Name:       "s1",
			ObjectType: byte('S'),
		})
	}
	fr.Send(&pgproto3.Sync{})
	if err := fr.Flush(); err != nil {
		return err
	}

	if err := waitRFQ(fr); err != nil {
		return err
	}
	fr.Send(&pgproto3.Bind{
		PreparedStatement: "s1",
	})

	fr.Send(&pgproto3.Describe{
		Name:       "",
		ObjectType: byte('P'),
	})
	fr.Send(&pgproto3.Execute{})
	fr.Send(&pgproto3.Sync{})
	if err := fr.Flush(); err != nil {
		return err
	}

	return waitRFQ(fr)
}

func gaogao() {
	conn, err := getC()
	if err != nil {
		if err != okerr {
			panic(err)
		}
		return
	}
	defer conn.Close()

	frontend := pgproto3.NewFrontend(conn, conn)
	frontend.Send(&pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters: map[string]string{
			"user":     "user1",
			"database": "db1",
			"password": "12345678",
		},
	})
	if err := frontend.Flush(); err != nil {
		log.Fatalf("startup failed %v", err)
		if err != okerr {
			panic(err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	if err := waitRFQ(frontend); err != nil {
		log.Fatalf("startup failed %v", err)
		if err != okerr {
			panic(err)
		}
		return
	}

	if err := prepBase(frontend); err != nil {
		log.Fatalf("startup failed %v", err)
		if err != okerr {
			panic(err)
		}
		return
	}

	log.Println("ok")
}

func main() {
	flag.Parse()

	gaogao()
}
