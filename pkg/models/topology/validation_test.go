package topology

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestValidateDataShardHostsAcceptsReachableHost(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer func() {
		_ = listener.Close()
	}()

	err = ValidateDataShardHosts(context.Background(), DataShardFromConfig("sh-ok", &config.Shard{
		RawHosts: []string{listener.Addr().String()},
		Type:     config.DataShard,
	}))
	assert.NoError(t, err)

	// Hosts with AZ
	err = ValidateDataShardHosts(context.Background(), DataShardFromConfig("sh-ok", &config.Shard{
		RawHosts: []string{fmt.Sprintf("%s:local", listener.Addr().String())},
		Type:     config.DataShard,
	}))
	assert.NoError(t, err)
}

func TestValidateDataShardHostsRejectsUnreachableHost(t *testing.T) {
	addr := func() string {
		const maxAttempts = 10
		for i := 0; i < maxAttempts; i++ {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			assert.NoError(t, err)
			candidate := listener.Addr().String()
			_ = listener.Close()

			conn, dialErr := net.DialTimeout("tcp", candidate, 100*time.Millisecond)
			if dialErr != nil {
				return candidate
			}
			_ = conn.Close()
		}
		t.Fatalf("failed to get an unreachable tcp address")
		return ""
	}()

	err := ValidateDataShardHosts(context.Background(), DataShardFromConfig("sh-bad", &config.Shard{
		RawHosts: []string{addr},
		Type:     config.DataShard,
	}))
	assert.ErrorContains(t, err, "not reachable")
}

func TestValidateDataShardHostsRejectsEmptyHosts(t *testing.T) {
	err := ValidateDataShardHosts(context.Background(), DataShardFromConfig("sh-empty", &config.Shard{
		RawHosts: []string{},
		Type:     config.DataShard,
	}))
	assert.ErrorContains(t, err, "has no valid hosts configured")
}

func TestValidateDataShardHostsRejectsUnsupportedHostFormat(t *testing.T) {
	err := ValidateDataShardHosts(context.Background(), DataShardFromConfig("sh-format", &config.Shard{
		RawHosts: []string{"host:1:az:extra"},
		Type:     config.DataShard,
	}))
	assert.ErrorContains(t, err, "invalid or unsupported host definitions")
}
