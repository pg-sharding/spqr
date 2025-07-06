package datashard

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/startup"

	"github.com/pg-sharding/spqr/pkg/auth"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

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

	dataPending bool

	tx_served int64

	id string

	status txstatus.TXStatus

	stmtDef  map[uint64]*prepstatement.PreparedStatementDefinition
	stmtDesc map[uint64]*prepstatement.PreparedStatementDescriptor
}

// ListPreparedStatements implements shard.Shard.
func (sh *Conn) ListPreparedStatements() []shard.PreparedStatementsMgrDescriptor {
	ret := make([]shard.PreparedStatementsMgrDescriptor, 0)

	for hash, def := range sh.stmtDef {

		ret = append(ret,
			shard.PreparedStatementsMgrDescriptor{
				Hash:     hash,
				ServerId: sh.ID(),
				Query:    def.Query,
				Name:     def.Name,
			},
		)
	}

	return ret
}

// Close closes the connection to the database.
// It returns an error if there was a problem closing the connection.
//
// Parameters:
// - None.
//
// Returns:
// - error: An error if the connection could not be closed.
func (sh *Conn) Close() error {
	return sh.dedicated.Close()
}

// Instance returns the dedicated database instance associated with the Conn struct.
//
// Parameters:
// - None.
//
// Returns:
// - conn.DBInstance: The dedicated database instance.
func (sh *Conn) Instance() conn.DBInstance {
	return sh.dedicated
}

// Sync returns the difference between the sync_out and sync_in fields of the Conn struct.
//
// Parameters:
// - None.
//
// Returns:
// - int64: The difference between sync_out and sync_in.
func (sh *Conn) Sync() int64 {
	return sh.sync_out - sh.sync_in
}

func (sh *Conn) DataPending() bool {
	return sh.dataPending
}

// TxServed returns the number of transactions served by the Conn struct.
//
// Parameters:
// - None.
//
// Returns:
// - int64: The number of transactions served.
func (sh *Conn) TxServed() int64 {
	return sh.tx_served
}

// TODO : unit tests

// Cancel cancels the current operation on the Conn struct.
//
// Parameters:
// - None.
//
// Returns:
// - error: An error if the cancel operation fails.
func (sh *Conn) Cancel() error {
	pgiTmp, err := conn.NewInstanceConn(
		sh.dedicated.Hostname(),
		sh.dedicated.AvailabilityZone(),
		sh.dedicated.ShardName(),
		nil, /* no tls for cancel */
		time.Second,
		time.Second,
		time.Millisecond*9500,
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := pgiTmp.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close pgiTmp")
		}
	}()

	msg := &pgproto3.CancelRequest{
		ProcessID: sh.backend_key_pid,
		SecretKey: sh.backend_key_secret,
	}

	spqrlog.Zero.Debug().
		Str("host", pgiTmp.Hostname()).
		Interface("msg", msg).
		Msg("sending cancel msg")

	return pgiTmp.Cancel(msg)
}

// TODO : unit tests

// Send sends a FrontendMessage to the shard connection.
//
// Parameters:
// - query (pgproto3.FrontendMessage): The query to be sent.
//
// Returns:
// - error: An error if the message cannot be sent.
func (sh *Conn) Send(query pgproto3.FrontendMessage) error {
	/* handle copy properly */

	sh.dataPending = true

	switch query.(type) {
	case *pgproto3.Query:
		sh.sync_in++
	case *pgproto3.Sync:
		sh.sync_in++
	default:
	}

	spqrlog.Zero.Debug().
		Uint("shard", sh.ID()).
		Interface("query", query).
		Int64("sync-in", sh.sync_in).
		Msg("shard connection send message")
	return sh.dedicated.Send(query)
}

// TODO : unit tests

// Receive receives a backend message from the connection.
//
// Parameters:
// - None.
//
// Returns:
// - pgproto3.BackendMessage: The received backend message.
// - error: An error if the message cannot be received.
func (sh *Conn) Receive() (pgproto3.BackendMessage, error) {
	msg, err := sh.dedicated.Receive()
	if err != nil {
		return nil, err
	}
	switch v := msg.(type) {
	case *pgproto3.ReadyForQuery:
		sh.dataPending = false
		sh.sync_out++
		sh.status = txstatus.TXStatus(v.TxStatus)
		if sh.status == txstatus.TXIDLE {
			sh.tx_served++
		}
	}

	spqrlog.Zero.Debug().
		Uint("shard", sh.ID()).
		Interface("msg", msg).
		Int64("sync-out", sh.sync_out).
		Msg("shard connection received message")
	return msg, nil
}

// String returns the name of the Conn struct as a string.
//
// Parameters:
// - None.
//
// Returns:
// - string: The name of the Conn struct.
func (sh *Conn) String() string {
	return sh.name
}

// Name returns the name of the Conn struct as a string.
//
// Parameters:
// - None.
//
// Returns:
// - string: The name of the Conn struct.
func (sh *Conn) Name() string {
	return sh.name
}

// Cfg returns the shard configuration.
//
// Parameters:
// - None.
//
// Returns:
// - *config.Shard: The shard configuration.
func (sh *Conn) Cfg() *config.Shard {
	return sh.cfg
}

// InstanceHostname returns the hostname of the instance associated with the Conn struct.
//
// It does this by calling the Instance() method of the Conn struct, which returns a pointer to a DBInstance struct.
// Then, it calls the Hostname() method of the DBInstance struct to retrieve the hostname.
//
// Parameters:
// - None.
//
// Returns:
// - string: The hostname of the instance associated with the Conn struct.
func (sh *Conn) InstanceHostname() string {
	return sh.Instance().Hostname()
}

func (sh *Conn) Pid() uint32 {
	return sh.backend_key_pid
}

// ShardKeyName returns the name of the shard key.
//
// It returns the name of the shard key by calling the SHKey method of the Conn struct and accessing the Name field of the returned ShardKey struct.
//
// Parameters:
// - None.
//
// Returns:
// - string: The name of the shard key.
func (sh *Conn) ShardKeyName() string {
	return sh.SHKey().Name
}

var _ shard.ShardHostInstance = &Conn{}

// SHKey returns the ShardKey associated with the Conn struct.
//
// It returns a ShardKey struct with the Name field set to the value of the Conn struct's name field.
//
// Parameters:
// - None.
//
// Returns:
// - kr.ShardKey: The ShardKey struct with the Name field set to the Conn struct's name field.
func (sh *Conn) SHKey() kr.ShardKey {
	return kr.ShardKey{
		Name: sh.name,
	}
}

// ID returns the unique identifier of the Conn struct.
//
// It returns an unsigned integer representing the ID of the Conn struct.
//
// Parameters:
// - None.
//
// Returns:
// - uint: The unique identifier of the Conn struct.
func (sh *Conn) ID() uint {
	return spqrlog.GetPointer(sh)
}

// Usr returns the username associated with the Conn struct.
//
// Parameters:
// - None.
//
// Returns:
// - string: The username from auth config or associated with the Conn struct.
func (sh *Conn) Usr() string {
	rule := sh.AuthRule()
	if rule != nil && rule.Usr != "" {
		return rule.Usr
	}
	return sh.beRule.Usr
}

// DB returns the database associated with the Conn struct.
//
// Parameters:
// - None.
//
// Returns:
// - string: The database associated with the Conn struct.
func (sh *Conn) DB() string {
	return sh.beRule.DB
}

// Params returns the ParameterSet associated with the Conn struct.
//
// Parameters:
// - None.
//
// Returns:
// - shard.ParameterSet: The ParameterSet associated with the Conn struct.
func (sh *Conn) Params() shard.ParameterSet {
	return sh.ps
}

// NewShard creates a new shard with the provided key, database instance, configuration, and backend rule.
//
// Parameters:
// - key (kr.ShardKey): The shard key.
// - pgi (conn.DBInstance): The database instance.
// - cfg (*config.Shard): The configuration for the shard.
// - beRule (*config.BackendRule): The backend rule for the shard.
//
// Returns:
// - shard.Shard: The newly created shard.
// - error: An error, if any.
func NewShard(
	key kr.ShardKey,
	pgi conn.DBInstance,
	cfg *config.Shard,
	beRule *config.BackendRule,
	sp *startup.StartupParams) (shard.ShardHostInstance, error) {

	dtSh := &Conn{
		cfg:       cfg,
		name:      key.Name,
		beRule:    beRule,
		ps:        shard.ParameterSet{},
		sync_in:   1, /* +1 for startup message */
		sync_out:  0,
		stmtDef:   map[uint64]*prepstatement.PreparedStatementDefinition{},
		stmtDesc:  map[uint64]*prepstatement.PreparedStatementDescriptor{},
		dedicated: pgi,
	}

	if dtSh.dedicated.Status() == conn.NotInitialized {
		if err := dtSh.Auth(sp); err != nil {
			return nil, err
		}
		dtSh.dedicated.SetStatus(conn.ACQUIRED)
	}

	return dtSh, nil
}

// Auth handles the authentication process for a shard connection.
//
// Parameters:
//   - sm (*pgproto3.StartupMessage): The startup message for the connection.
//
// Returns:
//   - error: An error if authentication fails.

// TODO : unit tests
func (sh *Conn) Auth(sp *startup.StartupParams) error {
	sm := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"application_name": "app",
			"client_encoding":  "UTF8",
			"user":             sh.Usr(),
			"database":         sh.beRule.DB,
		},
	}

	if sp.SearchPath != "" {
		sm.Parameters["search_path"] = sp.SearchPath
	}

	spqrlog.Zero.Debug().
		Uint("shard", sh.ID()).
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
				spqrlog.Zero.Error().Err(err).Msg("failed to perform backend auth")
				return err
			}
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("%s", v.Message)
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

// TODO : unit tests

// fire sends a query to the connection and processes the response.
//
// Parameters:
// - q (string): the query string to be sent.
//
// Returns:
// - error: an error if the query or response processing fails.
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
					sh.SetTxStatus(txstatus.TXStatus(v.TxStatus))
					return nil
				}
				return fmt.Errorf("unexpected tx status with rollback")
			}
		}
	}
}

// TODO : unit tests

// Cleanup cleans up the connection based on the provided rule.
//
// Parameters:
// - rule (*config.FrontendRule): a pointer to a config.FrontendRule object that contains the cleanup rules.
//
// Returns:
// - error: an error if there was a problem during cleanup, otherwise nil.
func (sh *Conn) Cleanup(rule *config.FrontendRule) error {
	if rule.PoolRollback {
		if sh.TxStatus() != txstatus.TXIDLE {
			if err := sh.fire("ROLLBACK"); err != nil {
				return err
			}
		}
	}

	if rule.PoolDiscard {
		if err := sh.fire("DISCARD ALL"); err != nil {
			return err
		}
	}

	return nil
}

// SetTxStatus sets the transaction status of the Conn object.
//
// Parameters:
// - tx (txstatus.TXStatus): the transaction status to be set.
//
// Returns:
//   - None.
func (sh *Conn) SetTxStatus(tx txstatus.TXStatus) {
	sh.status = tx
}

// TxStatus returns the transaction status of the Conn object.
//
// Parameters:
// - None.
//
// Returns:
// - txstatus.TXStatus: the transaction status.
func (sh *Conn) TxStatus() txstatus.TXStatus {
	return sh.status
}

// TODO : unit tests

// HasPrepareStatement checks if a prepared statement with the given hash exists in the Conn object.
//
// Parameters:
// - hash (uint64): the hash of the prepared statement.
//
// Returns:
// - bool: true if the prepared statement exists, false otherwise.
// - *shard.PreparedStatementDescriptor: the prepared statement descriptor, or nil if it does not exist.
func (srv *Conn) HasPrepareStatement(hash uint64, shardId uint) (bool, *prepstatement.PreparedStatementDescriptor) {
	if shardId != srv.ID() {
		return false, nil
	}
	rd, ok := srv.stmtDesc[hash]
	return ok, rd
}

// TODO : unit tests

// PrepareStatement adds a prepared statement to the Conn object.
//
// Parameters:
// - hash (uint64): the hash of the prepared statement.
// - rd (*shard.PreparedStatementDescriptor): the prepared statement descriptor.
//
// Returns:
// - None.
func (srv *Conn) StorePrepareStatement(hash uint64, shardId uint, def *prepstatement.PreparedStatementDefinition, rd *prepstatement.PreparedStatementDescriptor) error {
	id := srv.ID()
	if shardId != id {
		return spqrerror.Newf(spqrerror.SPQR_ROUTING_ERROR, "Cannot store stmt for shard \"%d\" in shard \"%d\"", shardId, id)
	}
	srv.stmtDef[hash] = def
	srv.stmtDesc[hash] = rd
	return nil
}

// AuthRule returns the backend auth configuration of the Conn object.
//
// Parameters:
// - None.
//
// Returns:
// - config.AuthBackendCfg: the configuration config.
func (sh *Conn) AuthRule() *config.AuthBackendCfg {
	var rule *config.AuthBackendCfg
	if sh.beRule.AuthRules == nil {
		rule = sh.beRule.DefaultAuthRule
	} else if _, exists := sh.beRule.AuthRules[sh.dedicated.ShardName()]; exists {
		rule = sh.beRule.AuthRules[sh.dedicated.ShardName()]
	} else {
		rule = sh.beRule.DefaultAuthRule
	}
	return rule
}
