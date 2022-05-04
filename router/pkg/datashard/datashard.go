package datashard

import (
	"crypto/tls"
	spqrlog "github.com/pg-sharding/spqr/pkg/spqrlog"
	"log"
	"sync"

	"github.com/jackc/pgproto3/v2"
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

	ConstructSM() *pgproto3.StartupMessage
	Instance() conn.DBInstance
}

func (sh *DataShardConn) ConstructSM() *pgproto3.StartupMessage {

	sm := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"application_name": "app",
			"client_encoding":  "UTF8",
			"user":             sh.beRule.RK.Usr,
			"database":         sh.beRule.RK.DB,
		},
	}
	return sm
}

type DataShardConn struct {
	lg log.Logger

	beRule *config.BERule
	cfg    *config.ShardCfg

	primary string
	name    string

	mu sync.Mutex

	dedicated conn.DBInstance
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

func NewShard(key kr.ShardKey, pgi conn.DBInstance, cfg *config.ShardCfg, beRule *config.BERule) (Shard, error) {

	dtSh := &DataShardConn{
		cfg:    cfg,
		name:   key.Name,
		beRule: beRule,
	}

	dtSh.dedicated = pgi

	if dtSh.dedicated.Status() == conn.NotInitialized {
		if err := dtSh.Auth(dtSh.ConstructSM()); err != nil {
			return nil, err
		}
		dtSh.dedicated.SetStatus(conn.ACQUIRED)
	}

	return dtSh, nil
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
				spqrlog.Logger.Errorf("failed to perform backend auth %w", err)
				return err
			}
		case *pgproto3.ErrorResponse:
			return xerrors.New(v.Message)
		case *pgproto3.ParameterStatus:
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "ignored parameter status %v %v", v.Name, v.Value)
		case *pgproto3.BackendKeyData:
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "ignored backend key data %v %v", v.ProcessID, v.SecretKey)
		default:
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "unexpected msg type received %T", v)
		}
	}
}
