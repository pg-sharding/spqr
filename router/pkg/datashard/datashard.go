package datashard

import (
	"crypto/tls"
	"log"
	"sync"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/asynctracelog"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type Shard interface {
	//Connect(proto string) (net.Conn, error)

	Cfg() *config.ShardCfg

	Name() string
	SHKey() kr.ShardKey

	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	ReqBackendSsl(tlscfg *tls.Config) error

	ConstructSMh() *pgproto3.StartupMessage
	Instance() conn.DBInstance
}

func (sh *DataShardConn) ConstructSMh() *pgproto3.StartupMessage {

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

type DataShardConn struct {
	cfg *config.ShardCfg

	lg log.Logger

	name string

	mu sync.Mutex

	dedicated conn.DBInstance

	primary string
}

func (sh *DataShardConn) Instance() conn.DBInstance {
	return sh.dedicated
}

func (sh *DataShardConn) ReqBackendSsl(tlscfg *tls.Config) error {
	if err := sh.dedicated.ReqBackendSsl(tlscfg); err != nil {
		tracelog.InfoLogger.Printf("failed to init ssl on host %v of datashard %v: %v", sh.dedicated.Hostname(), sh.Name(), err)

		return err
	}

	return nil
}

func (sh *DataShardConn) Send(query pgproto3.FrontendMessage) error {
	return sh.dedicated.Send(query)
}

func (sh *DataShardConn) Receive() (pgproto3.BackendMessage, error) {
	return sh.dedicated.Receive()
}

func (sh *DataShardConn) Name() string {
	return sh.name
}

func (sh *DataShardConn) Cfg() *config.ShardCfg {
	return sh.cfg
}

var _ Shard = &DataShardConn{}

func (sh *DataShardConn) SHKey() kr.ShardKey {
	return kr.ShardKey{
		Name: sh.name,
	}
}

func NewShard(key kr.ShardKey, pgi conn.DBInstance, cfg *config.ShardCfg) (Shard, error) {

	sh := &DataShardConn{
		cfg:  cfg,
		name: key.Name,
	}

	sh.dedicated = pgi

	if sh.dedicated.Status() == conn.NotInitialized {
		if err := sh.Auth(sh.ConstructSMh()); err != nil {
			return nil, err
		}
		sh.dedicated.SetStatus(conn.ACQUIRED)
	}

	return sh, nil
}

func (sh *DataShardConn) Auth(sm *pgproto3.StartupMessage) error {

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
		case pgproto3.AuthenticationResponseMessage:
			err := conn.AuthBackend(sh.dedicated, sh.Cfg(), v)
			if err != nil {
				asynctracelog.Printf("failed to perform backend auth %w", err)
				return err
			}
		case *pgproto3.ErrorResponse:
			return xerrors.New(v.Message)
		case *pgproto3.ParameterStatus:
			asynctracelog.Printf("ignored paramtes status %v %v", v.Name, v.Value)
		case *pgproto3.BackendKeyData:
			asynctracelog.Printf("ingored backend key data %v %v", v.ProcessID, v.SecretKey)
		default:
			asynctracelog.Printf("unexpected msg type received %T", v)
		}
	}
}
