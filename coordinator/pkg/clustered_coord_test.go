package coord

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

func TestClusteredCoordinatorAddDataShardStoresShardMetadata(t *testing.T) {
	db, err := qdb.NewMemQDB("")
	assert.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	assert.NoError(t, err)

	err = qc.AddDataShard(context.Background(), topology.DataShardFromConfig("sh-bad", &config.Shard{
		RawHosts: []string{"127.0.0.1:1"},
		Type:     config.DataShard,
	}))
	assert.NoError(t, err)

	sh, err := db.GetShard(context.Background(), "sh-bad")
	assert.NoError(t, err)
	assert.Equal(t, "sh-bad", sh.ID)
}
