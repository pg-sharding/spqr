package datashard

import (
	"crypto/tls"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/pg-sharding/spqr/pkg/auth"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

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

type Conn struct {
	beRule             *config.BackendRule
	cfg                *config.Shard
	name               string
	dedicated          conn.DBInstance
	ps                 shard.ParameterSet
	backend_key_pid    uint32
	backend_key_secret uint32

	sync_in  int64
	sync_out int64

	tx_served int64

	id string

	status txstatus.TXStatus
}

func (sh *Conn) Close() error {
	return sh.dedicated.Close()
}

func (sh *Conn) Instance() conn.DBInstance {
	return sh.dedicated
}

func (sh *Conn) Sync() int64 {
	return sh.sync_out - sh.sync_in
}

func (sh *Conn) TxServed() int64 {
	return sh.tx_served
}

func (sh *Conn) Cancel() error {
	pgiTmp, err := conn.NewInstanceConn(sh.dedicated.Hostname(), sh.dedicated.ShardName(), nil /* no tls for cancel */)
	if err != nil {
		return err
	}
	defer pgiTmp.Close()

	msg := &pgproto3.CancelRequest{
		ProcessID: sh.backend_key_pid,
		SecretKey: sh.backend_key_secret,
	}

	spqrlog.Zero.Debug().
		Str("host", pgiTmp.Hostname()).
		Interface("msg", msg).
		Msg("sendind cancel msg")

	return pgiTmp.Cancel(msg)
}

func (sh *Conn) AddTLSConf(tlsconfig *tls.Config) error {
	if err := sh.dedicated.ReqBackendSsl(tlsconfig); err != nil {
		spqrlog.Zero.Debug().
			Err(err).
			Str("host", sh.dedicated.Hostname()).
			Str("shard", sh.Name()).
			Msg("failed to init ssl on host of datashard")
		return err
	}
	return nil
}

func (sh *Conn) Send(query pgproto3.FrontendMessage) error {
	/* handle copy properly */
	sh.sync_in++

	spqrlog.Zero.Debug().
		Str("shard", sh.Name()).
		Interface("query", query).
		Int64("sync-in", sh.sync_in).
		Msg("shard connection send message")
	return sh.dedicated.Send(query)
}

func (sh *Conn) Receive() (pgproto3.BackendMessage, error) {
	msg, err := sh.dedicated.Receive()
	if err != nil {
		return nil, err
	}
	switch v := msg.(type) {
	case *pgproto3.ReadyForQuery:
		sh.sync_out++
		sh.status = txstatus.TXStatus(v.TxStatus)
		if sh.status == txstatus.TXIDLE {
			sh.tx_served++
		}
	}

	spqrlog.Zero.Debug().
		Str("shard", sh.Name()).
		Interface("msg", msg).
		Int64("sync-out", sh.sync_out).
		Msg("shard connection received message") 
	return msg, nil
}

func (sh *Conn) String() string {
	return sh.name
}

func (sh *Conn) Name() string {
	return sh.name
}

func (sh *Conn) Cfg() *config.Shard {
	return sh.cfg
}

var _ shard.Shard = &Conn{}

func (sh *Conn) SHKey() kr.ShardKey {
	return kr.ShardKey{
		Name: sh.name,
	}
}

func (sh *Conn) ID() string {
	return sh.id
}

func (sh *Conn) Usr() string {
	return sh.beRule.Usr
}

func (sh *Conn) DB() string {
	return sh.beRule.DB
}

func (sh *Conn) Params() shard.ParameterSet {
	return sh.ps
}

func NewShard(
	key kr.ShardKey,
	pgi conn.DBInstance,
	cfg *config.Shard,
	beRule *config.BackendRule) (shard.Shard, error) {

	dtSh := &Conn{
		cfg:      cfg,
		name:     key.Name,
		beRule:   beRule,
		ps:       shard.ParameterSet{},
		sync_in:  1, /* startup message */
		sync_out: 0,
	}

	dtSh.id = fmt.Sprintf("%p", dtSh)

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
	spqrlog.Zero.Debug().
		Str("shard", sh.Name()).
		Interface("msg", sm).
		Msg("shard connection startup message")
	if err := sh.dedicated.Send(sm); err != nil {
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
			err := auth.AuthBackend(sh.dedicated, sh.beRule, v)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("ailed to perform backend auth")
				return err
			}
		case *pgproto3.ErrorResponse:
			return fmt.Errorf(v.Message)
		case *pgproto3.ParameterStatus:
			if !sh.ps.Save(shard.ParameterStatus{
				Name:  v.Name,
				Value: v.Value,
			}) {
				spqrlog.Zero.Debug().
					Str("name", v.Name).
					Str("value", v.Value).
					Msg("ignored parameter status")
			} else {
				spqrlog.Zero.Debug().
					Str("name", v.Name).
					Str("value", v.Value).
					Msg("parameter status")
			}
		case *pgproto3.BackendKeyData:
			sh.backend_key_pid = v.ProcessID
			sh.backend_key_secret = v.SecretKey
			spqrlog.Zero.Debug().
				Uint32("process-id", v.ProcessID).
				Uint32("secret-key", v.SecretKey).
				Msg("backend key data")
		default:
			spqrlog.Zero.Debug().
				Type("type", v).
				Msg("unexpected msg type received")
		}
	}
}

func (sh *Conn) fire(q string) error {
	if err := sh.Send(&pgproto3.Query{
		String: q,
	}); err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Msg("error firing request to conn")
		return err
	}

	for {
		if msg, err := sh.Receive(); err != nil {
			return err
		} else {
			spqrlog.Zero.Debug().
				Str("shard", sh.id).
				Type("type", msg).
				Msg("shard rollback response")

			switch v := msg.(type) {
			case *pgproto3.ReadyForQuery:
				if v.TxStatus == byte(txstatus.TXIDLE) {
					return nil
				}
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

func (sh *Conn) SetTxStatus(tx txstatus.TXStatus) {
	sh.status = tx
}

func (sh *Conn) TxStatus() txstatus.TXStatus {
	return sh.status
}
