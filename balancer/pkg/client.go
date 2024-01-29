package pkg

import (
	"net"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/pg-sharding/spqr/pkg/client"
)

type BalancerClient struct {
	client.Client

	conn net.Conn
}

var _ client.Client = &BalancerClient{}

func NewBalancerClient(conn net.Conn) *BalancerClient {
	return &BalancerClient{
		conn: conn,
	}
}

func (c BalancerClient) Usr() string {
	return "DefaultUsr"
}

func (c BalancerClient) DB() string {
	return "DefaultDB"
}

func (c BalancerClient) Distribution() string {
	return "default"
}

func (c BalancerClient) DistributionIsDefault() bool {
	return true
}

func (c BalancerClient) SetDS(_ string) {}

func (c BalancerClient) ID() uint {
	return 0x123456789
}

func (c BalancerClient) Receive() (pgproto3.FrontendMessage, error) {
	return &pgproto3.Query{}, nil
}

func (c BalancerClient) Send(msg pgproto3.BackendMessage) error {
	return nil
}
