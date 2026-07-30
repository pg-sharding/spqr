package coord

import (
	"context"
	"errors"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

type taskGroupResultQDB struct {
	qdb.XQDB
	droppedTaskGroupLock string
}

func (db *taskGroupResultQDB) DropTaskGroupLock(_ context.Context, id string) error {
	db.droppedTaskGroupLock = id
	return nil
}

func TestClusteredCoordinatorAddDataShardStoresShardMetadata(t *testing.T) {
	db, err := qdb.NewMemQDB("")
	assert.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	assert.NoError(t, err)

	err = qc.AddDataShard(context.Background(), topology.DataShardFromConfig("sh-bad", &config.Shard{
		RawHosts: []string{"127.0.0.1:1"},
		Type:     config.DataShard,
	}), true)
	assert.NoError(t, err)

	sh, err := db.GetShard(context.Background(), "sh-bad")
	assert.NoError(t, err)
	assert.Equal(t, "sh-bad", sh.ID)
}

func TestAwaitMoveTaskGroupResultUpdatesErrorStatus(t *testing.T) {
	memDB, err := qdb.NewMemQDB("")
	assert.NoError(t, err)
	db := &taskGroupResultQDB{XQDB: memDB}

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	assert.NoError(t, err)

	taskErr := errors.New("move failed")
	resultCh := make(chan error, 1)
	resultCh <- taskErr

	err = qc.awaitMoveTaskGroupResult(context.Background(), "task-group-id", resultCh)
	assert.ErrorIs(t, err, taskErr)
	assert.Equal(t, "task-group-id", db.droppedTaskGroupLock)

	status, err := memDB.GetTaskGroupStatus(context.Background(), "task-group-id")
	assert.NoError(t, err)
	if assert.NotNil(t, status) {
		assert.Equal(t, string(tasks.TaskGroupError), status.State)
		assert.Equal(t, taskErr.Error(), status.Message)
	}
}
