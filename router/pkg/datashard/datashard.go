package datashard

import (
	"crypto/tls"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
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
	Cfg() *config.ShardCfg

	Name() string
	SHKey() kr.ShardKey

	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	ReqBackendSsl(tlscfg *tls.Config) error

	ConstructSM() *pgproto3.StartupMessage
	Instance() conn.DBInstance

	Params() ParameterSet
}

func (sh *Conn) ConstructSM() *pgproto3.StartupMessage {
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

type ParameterStatus struct {
	Name  string
	Value string
}

type ParameterSet map[string]string

func (ps ParameterSet) Save(status ParameterStatus) bool {
	if _, ok := ps[status.Name]; ok {
		return false
	}
	ps[status.Name] = status.Value
	return true
}

type Conn struct {
	lg log.Logger

	beRule *config.BERule
	cfg    *config.ShardCfg

	primary string
	name    string

	mu sync.Mutex

	dedicated conn.DBInstance
	ps        ParameterSet
}

func (sh *Conn) Instance() conn.DBInstance {
	return sh.dedicated
}

func (sh *Conn) ReqBackendSsl(tlscfg *tls.Config) error {
	if err := sh.dedicated.ReqBackendSsl(tlscfg); err != nil {
		tracelog.InfoLogger.Printf("failed to init ssl on host %v of datashard %v: %v", sh.dedicated.Hostname(), sh.Name(), err)

		return err
	}

	return nil
}

func (sh *Conn) Send(query pgproto3.FrontendMessage) error {
	return sh.dedicated.Send(query)
}

func (sh *Conn) Receive() (pgproto3.BackendMessage, error) {
	return sh.dedicated.Receive()
}

func (sh *Conn) Name() string {
	return sh.name
}

func (sh *Conn) Cfg() *config.ShardCfg {
	return sh.cfg
}

var _ Shard = &Conn{}

func (sh *Conn) SHKey() kr.ShardKey {
	return kr.ShardKey{
		Name: sh.name,
	}
}

func (sh *Conn) Params() ParameterSet {
	return sh.ps
}

func NewShard(key kr.ShardKey, pgi conn.DBInstance, cfg *config.ShardCfg, beRule *config.BERule) (Shard, error) {
	dtSh := &Conn{
		cfg:    cfg,
		name:   key.Name,
		beRule: beRule,
		ps:     ParameterSet{},
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

func (sh *Conn) Auth(sm *pgproto3.StartupMessage) error {

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
			if !sh.ps.Save(ParameterStatus{
				Name:  v.Name,
				Value: v.Value,
			}) {
				spqrlog.Logger.Printf(spqrlog.DEBUG1, "ignored parameter status %v %v", v.Name, v.Value)
			}
		case *pgproto3.BackendKeyData:
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "ignored backend key data %v %v", v.ProcessID, v.SecretKey)
		default:
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "unexpected msg type received %T", v)
		}
	}
}
