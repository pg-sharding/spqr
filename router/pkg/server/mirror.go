package server

import (
	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/router/pkg/datashard"
)

func NewMultiShardServer(rule *config.BERule, pool conn.ConnPool) (Server, error) {
	ret := &MultiShardServer{
		rule:         rule,
		pool:         pool,
		activeShards: []datashard.Shard{},
	}

	return ret, nil
}

type LoadMirroringServer struct {
	Server
	main   Server
	mirror Server
}

var _ Server = &LoadMirroringServer{}

func NewLoadMirroringServer(source Server, dest Server) *LoadMirroringServer {
	return &LoadMirroringServer{
		main:   source,
		mirror: dest,
	}
}

func (LoadMirroringServer) Send(query pgproto3.FrontendMessage) error {
	return nil
}
func (LoadMirroringServer) Receive() (pgproto3.BackendMessage, error) {
	return nil, nil
}
