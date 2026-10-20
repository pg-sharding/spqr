package pool

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/stretchr/testify/require"
)

// startProbeBackend runs a real PostgreSQL wire connection and reports when the
// probe has closed it. Server-side deadlines also bound cleanup if a test fails.
func startProbeBackend(t *testing.T, withTLS bool, serve func(*pgproto3.Backend, *pgproto3.StartupMessage) error) (string, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	var tlsconfig *tls.Config
	if withTLS {
		certificate, err := tls.LoadX509KeyPair("../../docker/router/ssl/localhost.crt", "../../docker/router/ssl/localhost.key")
		require.NoError(t, err)
		tlsconfig = &tls.Config{Certificates: []tls.Certificate{certificate}}
	}
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		c, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		backend := pgproto3.NewBackend(c, c)
		if withTLS {
			if _, err := backend.ReceiveStartupMessage(); err != nil {
				return
			}
			if _, err := c.Write([]byte{'S'}); err != nil {
				return
			}
			secure := tls.Server(c, tlsconfig)
			if err := secure.Handshake(); err != nil {
				return
			}
			c = secure
			backend = pgproto3.NewBackend(c, c)
		}
		message, err := backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
		startupMessage, ok := message.(*pgproto3.StartupMessage)
		if !ok {
			return
		}
		if err := serve(backend, startupMessage); err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, c)
	}()
	return listener.Addr().String(), closed
}

func probeTopology(address string, withTLS bool) topology.TopologyMgr {
	options := []topology.GenericOption{{Name: "host", Arg: address}}
	if withTLS {
		options = append(options, topology.TLSConfigToOptions(&config.TLSConfig{SslMode: "require"})...)
	}
	return topology.TopMgrFromMap(map[string]*topology.DataShard{"shard": topology.NewDataShard("shard", config.DataShard, options)})
}

func probeBackendReady(backend *pgproto3.Backend) error {
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return err
	}
	message, err := backend.Receive()
	if err != nil {
		return err
	}
	query, ok := message.(*pgproto3.Query)
	if !ok || query.String != "SHOW transaction_read_only" {
		return fmt.Errorf("unexpected query: %v", message)
	}
	return nil
}

func TestPrimaryProbeUsesBackendAuthAndTLS(t *testing.T) {
	for _, withTLS := range []bool{false, true} {
		for _, readOnly := range []string{"on", "off"} {
			t.Run(fmt.Sprintf("tls=%t/%s", withTLS, readOnly), func(t *testing.T) {
				address, closed := startProbeBackend(t, withTLS, func(backend *pgproto3.Backend, startupMessage *pgproto3.StartupMessage) error {
					if startupMessage.Parameters["user"] != "backend-user" || startupMessage.Parameters["database"] != "database" || startupMessage.Parameters["search_path"] != "monitor" {
						return fmt.Errorf("unexpected startup parameters: %v", startupMessage.Parameters)
					}
					backend.Send(&pgproto3.AuthenticationCleartextPassword{})
					if err := backend.Flush(); err != nil {
						return err
					}
					if err := backend.SetAuthType(pgproto3.AuthTypeCleartextPassword); err != nil {
						return err
					}
					password, err := backend.Receive()
					if err != nil {
						return err
					}
					if password, ok := password.(*pgproto3.PasswordMessage); !ok || password.Password != "secret" {
						return fmt.Errorf("unexpected authentication response")
					}
					if err := probeBackendReady(backend); err != nil {
						return err
					}
					backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(readOnly)}})
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SHOW")})
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					return backend.Flush()
				})
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				rule := &config.BackendRule{Usr: "route-user", DB: "database", DefaultAuthRule: &config.AuthBackendCfg{Usr: "backend-user", Password: "secret"}}
				result, stage, err := probePrimary(ctx, probeTopology(address, withTLS), &startup.StartupParams{SearchPath: "monitor"}, rule, kr.ShardKey{Name: "shard"}, config.Host{Address: address})
				require.NoError(t, err)
				require.Empty(t, stage)
				require.True(t, result.Alive)
				require.Equal(t, readOnly == "off", result.RW)
				select {
				case <-closed:
				case <-time.After(time.Second):
					t.Fatal("successful probe connection leaked")
				}
			})
		}
	}
}

func TestPrimaryProbeDeadlineIsNotRenewedByMessages(t *testing.T) {
	for _, stage := range []string{"authentication", "query"} {
		t.Run(stage, func(t *testing.T) {
			address, closed := startProbeBackend(t, false, func(backend *pgproto3.Backend, _ *pgproto3.StartupMessage) error {
				if stage == "query" {
					if err := probeBackendReady(backend); err != nil {
						return err
					}
				}
				// Useful protocol traffic cannot extend the absolute probe deadline.
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				for range ticker.C {
					backend.Send(&pgproto3.ParameterStatus{Name: "application_name", Value: "probe"})
					if err := backend.Flush(); err != nil {
						return err
					}
				}
				return nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			started := time.Now()
			result, failureStage, err := probePrimary(ctx, probeTopology(address, false), nil, &config.BackendRule{Usr: "user", DB: "db"}, kr.ShardKey{Name: "shard"}, config.Host{Address: address})
			require.Error(t, err)
			require.False(t, result.Alive)
			require.Equal(t, stage, failureStage)
			require.Less(t, time.Since(started), time.Second)
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("timed out probe connection leaked")
			}
		})
	}
}

func TestPrimaryProbePublishesActionableProtocolFailures(t *testing.T) {
	for _, phase := range []string{"authentication", "query", "missing-result"} {
		t.Run(phase, func(t *testing.T) {
			address, closed := startProbeBackend(t, false, func(backend *pgproto3.Backend, _ *pgproto3.StartupMessage) error {
				if phase != "authentication" {
					if err := probeBackendReady(backend); err != nil {
						return err
					}
				}
				if phase == "missing-result" {
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				} else {
					backend.Send(&pgproto3.ErrorResponse{Code: "28000", Message: "permission denied"})
				}
				return backend.Flush()
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result, stage, err := probePrimary(ctx, probeTopology(address, false), nil, &config.BackendRule{Usr: "user", DB: "db"}, kr.ShardKey{Name: "shard"}, config.Host{Address: address})
			require.Error(t, err)
			require.False(t, result.Alive)
			if phase == "authentication" {
				require.Equal(t, "authentication", stage)
			} else {
				require.Equal(t, "query", stage)
			}
			if phase == "missing-result" {
				require.Contains(t, result.Reason, "no transaction_read_only result")
			} else {
				require.Contains(t, result.Reason, "permission denied")
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("failed probe connection leaked")
			}
		})
	}
}
