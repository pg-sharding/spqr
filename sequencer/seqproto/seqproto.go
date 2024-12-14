package seqproto

import (
	"net"

	"github.com/pg-sharding/spqr/pkg/config"
)

type MessageType int

const (
	Create     = MessageType(26)
	CurrentVal = MessageType(27)
	NextVal    = MessageType(28)
)

type NetProtoInteractor interface {
	/* type, body, error if any */
	DecodeMessage() (MessageType, []byte, error)

	AcceptClient() (net.Conn, error)
}

type PostgreSQLStorage struct {
	scfg *config.Sequencer
}

// DecodeMessage implements NetProtoInteractor.
func (p *PostgreSQLStorage) DecodeMessage() (MessageType, []byte, error) {
	panic("unimplemented")
}

func (p *PostgreSQLStorage) Init() error {
	return nil
}

func NewNetProtoInteractor(scfg *config.Sequencer) (NetProtoInteractor, error) {
	ret := &PostgreSQLStorage{
		scfg: scfg,
	}

	if err := ret.Init(); err != nil {
		return nil, err
	}

	return ret, nil
}
