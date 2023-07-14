package main

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/jackc/pgx/v5/pgproto3"
)

func getC() (net.Conn, error) {
	const proto = "tcp"
	const addr = "[::1]:6432"
	return net.Dial(proto, addr)
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	conn, err := getC()
	if err != nil {
		fmt.Printf("failed %v", err)
		return
	}

	frontend := pgproto3.NewFrontend(conn, conn)
	sm := pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters: map[string]string{
			"database": "db1",
			"user":     "user1",
		},
	}
	frontend.Send(&sm)
	if err := frontend.Flush(); err != nil {
		fmt.Printf("failed %v", err)
		return
	}

	r, err := frontend.Receive()
	if err != nil {
		fmt.Printf("failed %v", err)
		return
	}
	fmt.Printf("resp: %v\n", r)

	for {
		fmt.Print("Enter prepared stmt name: ")
		//		fmt.Print("~$ ")
		name, _ := reader.ReadString('\n')

		fmt.Print("Enter prepared stmt query: ")
		//		fmt.Print("~$ ")
		query, _ := reader.ReadString('\n')

		msg := &pgproto3.Parse{
			Name:  name,
			Query: query,
		}
		frontend.Send(msg)
		if err := frontend.Flush(); err != nil {
			fmt.Printf("failed %v", err)
			return
		}

		_, err = frontend.Receive()
		if err != nil {
			fmt.Printf("failed %v", err)
			return
		}

		fmt.Print("Enter for exec: ")
		_, _ = reader.ReadString('\n')

		msg2 := &pgproto3.Describe{
			ObjectType: 'S',
			Name:       name,
		}
		frontend.Send(msg2)
		if err := frontend.Flush(); err != nil {
			fmt.Printf("failed %v", err)
			return
		}

		r, err := frontend.Receive()
		if err != nil {
			fmt.Printf("failed %v", err)
			return
		}
		fmt.Printf("resp: %v\n", r)
	}
}
