package conn

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestContextConnectionBoundsIOAndClosesOnCancellation(t *testing.T) {
	for _, operation := range []string{"read", "write"} {
		for _, cancellation := range []string{"deadline", "cancel"} {
			t.Run(operation+"/"+cancellation, func(t *testing.T) {
				client, server := net.Pipe()
				defer func() { _ = server.Close() }()
				ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
				defer cancel()
				instance, err := newInstanceWithContext(ctx, client, "host", "az", "shard", nil)
				require.NoError(t, err)
				defer func() { _ = instance.Close() }()
				finished := make(chan error, 1)
				go func() {
					if operation == "write" {
						finished <- instance.Send(&pgproto3.Query{String: strings.Repeat("x", 1024)})
					} else {
						_, err := instance.Receive()
						finished <- err
					}
				}()
				if cancellation == "cancel" {
					cancel()
				}
				select {
				case err := <-finished:
					require.Error(t, err)
				case <-time.After(time.Second):
					t.Fatal("socket operation outlived the probe context")
				}
				_ = instance.Close()
				_, err = server.Read(make([]byte, 1))
				require.ErrorIs(t, err, io.EOF)
			})
		}
	}
}

func TestContextConnectionBoundsTLSAndClosesFailures(t *testing.T) {
	for _, stage := range []string{"ssl-response", "handshake"} {
		t.Run(stage, func(t *testing.T) {
			client, server := net.Pipe()
			defer func() { _ = server.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer cancel()
			closed := make(chan error, 1)
			go func() {
				request := make([]byte, 8)
				if _, err := io.ReadFull(server, request); err != nil {
					closed <- err
					return
				}
				if stage == "handshake" {
					if _, err := server.Write([]byte{'S'}); err != nil {
						closed <- err
						return
					}
				}
				_, err := io.Copy(io.Discard, server)
				closed <- err
			}()
			instance, err := newInstanceWithContext(ctx, client, "host", "az", "shard", &tls.Config{InsecureSkipVerify: true})
			require.Nil(t, instance)
			var phase *ConnectionError
			require.ErrorAs(t, err, &phase)
			require.Equal(t, "tls", phase.Stage)
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("failed TLS connection was left open")
			}
		})
	}
}

func TestContextConnectionRejectsCancelledDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, host := range []string{"unused.invalid:5432", "192.0.2.1:5432"} {
		instance, err := NewInstanceConnContext(ctx, host, "", "shard", nil, 0, 0)
		require.Nil(t, instance)
		require.True(t, errors.Is(err, context.Canceled))
	}
}

func TestContextConnectionBoundsDNSResolution(t *testing.T) {
	previousResolver := net.DefaultResolver
	started := make(chan struct{}, 1)
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	defer func() { net.DefaultResolver = previousResolver }()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	start := time.Now()
	instance, err := NewInstanceConnContext(ctx, "primary-probe.invalid:5432", "", "shard", nil, 0, 0)
	require.Nil(t, instance)
	require.Error(t, err)
	require.Less(t, time.Since(start), time.Second)
	select {
	case <-started:
	default:
		t.Fatal("DNS resolver was not exercised")
	}
}

func TestTCPUserTimeoutControlIsPlatformSpecific(t *testing.T) {
	require.Nil(t, setTCPUserTimeout(0))
	if runtime.GOOS == "linux" {
		require.NotNil(t, setTCPUserTimeout(time.Second))
	} else {
		require.Nil(t, setTCPUserTimeout(time.Second))
	}
}
