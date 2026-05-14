package coord

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
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

func TestClusteredCoordinatorExecuteMoveInternalWritesErrorStatus(t *testing.T) {
	db, err := qdb.NewMemQDB("")
	assert.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	assert.NoError(t, err)
	qc.moveTaskWatcherInit.Do(func() {})

	tg := &tasks.MoveTaskGroup{
		ID:        "tg-error",
		KridFrom:  "missing-kr",
		TotalKeys: 17,
		BatchSize: 17,
	}

	err = qc.executeMoveInternal(context.Background(), tg, false, false)
	assert.Error(t, err)

	status, statusErr := db.GetTaskGroupStatus(context.Background(), tg.ID)
	assert.NoError(t, statusErr)
	assert.NotNil(t, status)
	assert.Equal(t, string(tasks.TaskGroupError), status.State)
	assert.Equal(t, err.Error(), status.Message)
	assert.Equal(t, stageError, status.Stage)
	assert.Equal(t, err.Error(), status.Detail)
	assert.Equal(t, tg.TotalKeys, status.KeysProcessed)
	assert.False(t, status.UpdatedAt.IsZero())
}
