package rrouter

import (
	"crypto/tls"
	"log"
	"sync"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/coordinator/qdb/qdb"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/conn"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type Shard interface {
	//Connect(proto string) (net.Conn, error)

	Cfg() *config.ShardCfg

	Name() string
	SHKey() qdb.ShardKey

	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	ReqBackendSsl(tlscfg *tls.Config) error

	ConstructSMh() *pgproto3.StartupMessage
	Instance() conn.DBInstance
}

func (sh *ShardImpl) ConstructSMh() *pgproto3.StartupMessage {

	sm := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"application_name": "app",
			"client_encoding":  "UTF8",
			"user":             sh.cfg.ConnUsr,
			"database":         sh.cfg.ConnDB,
		},
	}
	return sm
}

type ShardImpl struct {
	cfg *config.ShardCfg

	lg log.Logger

	name string

	mu sync.Mutex

	dedicated conn.DBInstance

	primary string
}

func (sh *ShardImpl) Instance() conn.DBInstance {
	return sh.dedicated
}

func (sh *ShardImpl) ReqBackendSsl(tlscfg *tls.Config) error {
	if err := sh.dedicated.ReqBackendSsl(tlscfg); err != nil {
		tracelog.InfoLogger.Printf("failed to init ssl on host %v of shard %v: %v", sh.dedicated.Hostname(), sh.Name(), err)

		return err
	}

	return nil
}

func (sh *ShardImpl) Send(query pgproto3.FrontendMessage) error {
	return sh.dedicated.Send(query)
}

func (sh *ShardImpl) Receive() (pgproto3.BackendMessage, error) {
	return sh.dedicated.Receive()
}

func (sh *ShardImpl) Name() string {
	return sh.name
}

func (sh *ShardImpl) Cfg() *config.ShardCfg {
	return sh.cfg
}

var _ Shard = &ShardImpl{}

func (sh *ShardImpl) SHKey() qdb.ShardKey {
	return qdb.ShardKey{
		Name: sh.name,
	}
}

func NewShard(key qdb.ShardKey, pgi conn.DBInstance, cfg *config.ShardCfg) (Shard, error) {

	sh := &ShardImpl{
		cfg:  cfg,
		name: key.Name,
	}

	sh.dedicated = pgi

	if err := sh.Auth(sh.ConstructSMh()); err != nil {
		return nil, err
	}

	return sh, nil
}

func (sh *ShardImpl) Auth(sm *pgproto3.StartupMessage) error {

	err := sh.dedicated.Send(sm)
	if err != nil {
		return err
	}

	for {
		msg, err := sh.Receive()
		if err != nil {
			return err
		}
		switch v := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return nil
		case *pgproto3.Authentication:
			err := authBackend(sh, v)
			if err != nil {
				tracelog.InfoLogger.Printf("failed to perform backend auth %w", err)
				return err
			}
		case *pgproto3.ErrorResponse:
			return xerrors.New(v.Message)
		case *pgproto3.ParameterStatus:

			tracelog.InfoLogger.Printf("ignored paramtes status %v %v", v.Name, v.Value)

		case *pgproto3.BackendKeyData:
			tracelog.InfoLogger.Printf("ingored backend key data %v %v", v.ProcessID, v.SecretKey)
		default:
			tracelog.InfoLogger.Printf("unexpected msg type received %T", v)
		}
	}
}
