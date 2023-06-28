package server

import (
	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/datashard"
	"github.com/pg-sharding/spqr/pkg/shard"
)

func NewMultiShardServer(rule *config.BackendRule, pool datashard.DBPool) (Server, error) {
	ret := &MultiShardServer{
		rule:         rule,
		pool:         pool,
		activeShards: []shard.Shard{},
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

func (m *LoadMirroringServer) Datashards() []shard.Shard {
	return []shard.Shard{}
}
