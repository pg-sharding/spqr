package conn

import "net"

//go:generate -command mockgen -source=pkg/conn/raw.go -destination=pkg/mock/conn/raw_mock.go -package=mock_conn
type RawConn interface {
	net.Conn
}
