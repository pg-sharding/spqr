package coord

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

func TestClusteredCoordinatorAddDataShardRejectsUnreachableHosts(t *testing.T) {
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

	db, err := qdb.NewMemQDB("")
	assert.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db)
	assert.NoError(t, err)

	err = qc.AddDataShard(context.Background(), topology.NewDataShard("sh-bad", &config.Shard{
		RawHosts: []string{addr},
		Type:     config.DataShard,
	}))
	assert.ErrorContains(t, err, "not reachable")

	_, err = db.GetShard(context.Background(), "sh-bad")
	assert.Error(t, err)
}
