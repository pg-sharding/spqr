package server

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
)

func NewMultiShardServer(pool pool.DBPool) (Server, error) {
	ret := &MultiShardServer{
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
