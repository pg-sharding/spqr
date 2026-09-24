package coord

import (
	"context"
	"errors"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/qdb"
	mockqdb "github.com/pg-sharding/spqr/qdb/mock"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
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

func TestAlterReferenceRelationStoragePreservesError(t *testing.T) {
	ctx := context.Background()
	name := &rfqn.RelationFQN{SchemaName: "sales", RelationName: "countries"}
	for _, tt := range []struct {
		name    string
		storage []string
	}{
		{"remove shard", []string{"sh1"}},
		{"sync shard", []string{"sh1", "sh2", "sh3"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := mockqdb.NewMockXQDB(gomock.NewController(t))
			original := spqrerror.ReferenceRelationNotFound(name.String()).Detail("metadata changed during the operation")
			lookup := db.EXPECT().GetReferenceRelation(ctx, name).Return(&qdb.ReferenceRelation{TableName: "countries", SchemaName: "sales", ShardIDs: []string{"sh1", "sh2"}}, nil)
			if tt.name == "remove shard" {
				db.EXPECT().AlterReferenceRelationStorage(ctx, name, tt.storage).Return(original)
			} else {
				db.EXPECT().GetReferenceRelation(ctx, name).After(lookup).Return(nil, original)
			}
			qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
			require.NoError(t, err)
			err = qc.AlterReferenceRelationStorageAdvanced(ctx, name, tt.storage)
			require.ErrorIs(t, err, original)
			assert.Contains(t, err.Error(), "failed to alter reference relation storage")
		})
	}
}
