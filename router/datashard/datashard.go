package datashard

import (
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type Shard interface {
	Cfg() *config.Shard

	Name() string
	SHKey() kr.ShardKey

	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	AddTLSConf(cfg *tls.Config) error
	Cleanup(rule *config.FrontendRule) error

	ConstructSM() *pgproto3.StartupMessage
	Instance() conn.DBInstance

	Cancel() error

	Params() ParameterSet
	Close() error
}

func (sh *Conn) ConstructSM() *pgproto3.StartupMessage {
	sm := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"application_name": "app",
			"client_encoding":  "UTF8",
			"user":             sh.beRule.Usr,
			"database":         sh.beRule.DB,
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
	beRule             *config.BackendRule
	cfg                *config.Shard
	name               string
	mu                 sync.Mutex
	dedicated          conn.DBInstance
	ps                 ParameterSet
	backend_key_pid    uint32
	backend_key_secret uint32
}

func (sh *Conn) Close() error {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.dedicated.Close()
}

func (sh *Conn) Instance() conn.DBInstance {
	return sh.dedicated
}

func (sh *Conn) Cancel() error {
	pgiTmp, err := conn.NewInstanceConn(sh.dedicated.Hostname(), nil /* no tls for cancel */)
	if err != nil {
		return err
	}
	defer pgiTmp.Close()

	msg := &pgproto3.CancelRequest{
		ProcessID: sh.backend_key_pid,
		SecretKey: sh.backend_key_secret,
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "sendind cancel msg %v over %p", msg, &pgiTmp)

	return pgiTmp.Cancel(msg)
}

func (sh *Conn) AddTLSConf(tlsconfig *tls.Config) error {
	if err := sh.dedicated.ReqBackendSsl(tlsconfig); err != nil {
		spqrlog.Logger.Printf(spqrlog.DEBUG3, "failed to init ssl on host %v of datashard %v: %v", sh.dedicated.Hostname(), sh.Name(), err)
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

func (sh *Conn) Cfg() *config.Shard {
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

func NewShard(key kr.ShardKey, pgi conn.DBInstance, cfg *config.Shard, beRule *config.BackendRule) (Shard, error) {
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
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "shard conn %p startup msg: %+v", sh, sm)
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
			err := conn.AuthBackend(sh.dedicated, sh.beRule, v)
			if err != nil {
				spqrlog.Logger.Errorf("failed to perform backend auth %v", err)
				return err
			}
		case *pgproto3.ErrorResponse:
			return fmt.Errorf(v.Message)
		case *pgproto3.ParameterStatus:
			if !sh.ps.Save(ParameterStatus{
				Name:  v.Name,
				Value: v.Value,
			}) {
				spqrlog.Logger.Printf(spqrlog.DEBUG1, "ignored parameter status %v %v", v.Name, v.Value)
			} else {
				spqrlog.Logger.Printf(spqrlog.DEBUG5, "parameter status %v %v", v.Name, v.Value)
			}
		case *pgproto3.BackendKeyData:
			sh.backend_key_pid = v.ProcessID
			sh.backend_key_secret = v.SecretKey
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "backend key data %v %v", v.ProcessID, v.SecretKey)
		default:
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "unexpected msg type received %T", v)
		}
	}
}

func (sh *Conn) fire(q string) error {
	if err := sh.Send(&pgproto3.Query{
		String: q,
	}); err != nil {
		spqrlog.Logger.Printf(spqrlog.DEBUG1, "error firing request to conn")
		return err
	}

	for {
		if msg, err := sh.Receive(); err != nil {
			return err
		} else {
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "rollback resp %T", msg)

			switch msg.(type) {
			case *pgproto3.ReadyForQuery:
				return nil
			}
		}
	}
}

func (sh *Conn) Cleanup(rule *config.FrontendRule) error {
	if rule.PoolRollback {
		if err := sh.fire("ROLLBACK"); err != nil {
			return err
		}
	}

	if rule.PoolDiscard {
		if err := sh.fire("DISCARD ALL"); err != nil {
			return err
		}
	}

	return nil
}
