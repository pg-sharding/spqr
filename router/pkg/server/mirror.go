package server

import (
	"github.com/jackc/pgproto3/v2"
)

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
