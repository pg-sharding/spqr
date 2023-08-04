package conn

import "net"

type RawConn interface {
	net.Conn
}
