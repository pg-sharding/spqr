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
	"github.com/stretchr/testify/require"
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

func TestClusteredCoordinatorAddRouter(t *testing.T) {
	ctx := context.Background()

	db, err := qdb.NewMemQDB("")
	require.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	require.NoError(t, err)

	router := &topology.Router{
		ID:      "router-1",
		Address: "127.0.0.1:6432",
	}

	err = qc.AddRouter(ctx, router)
	require.NoError(t, err)

	routers, err := db.ListRouters(ctx)
	require.NoError(t, err)

	require.Len(t, routers, 1)

	assert.Equal(t, router.ID, routers[0].ID)
	assert.Equal(t, router.Address, routers[0].Address)
	assert.Equal(t, router.State, routers[0].State)
}

func TestClusteredCoordinatorAddMultipleRouters(t *testing.T) {
	ctx := context.Background()

	db, err := qdb.NewMemQDB("")
	require.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	require.NoError(t, err)

	routers := []*topology.Router{
		{ID: "router-1", Address: "127.0.0.1:6432"},
		{ID: "router-2", Address: "127.0.0.1:6433"},
	}

	for _, router := range routers {
		require.NoError(t, qc.AddRouter(ctx, router))
	}

	actual, err := db.ListRouters(ctx)
	require.NoError(t, err)
	require.Len(t, actual, 2)

	assert.Equal(t, routers[0].ID, actual[0].ID)
	assert.Equal(t, routers[1].ID, actual[1].ID)
}

func TestClusteredCoordinatorAddRouterDuplicateID(t *testing.T) {
	ctx := context.Background()

	db, err := qdb.NewMemQDB("")
	require.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	require.NoError(t, err)

	router1 := &topology.Router{
		ID:      "router-1",
		Address: "127.0.0.1:6432",
	}

	router2 := &topology.Router{
		ID:      "router-1",
		Address: "127.0.0.1:6433",
	}

	require.NoError(t, qc.AddRouter(ctx, router1))
	require.NoError(t, qc.AddRouter(ctx, router2))

	routers, err := db.ListRouters(ctx)
	require.NoError(t, err)

	require.Len(t, routers, 1)
	assert.Equal(t, "router-1", routers[0].ID)
	assert.Equal(t, "127.0.0.1:6433", routers[0].Address)
}

type errorAddRouterQDB struct {
	qdb.XQDB
	addRouterErr error
}

func (db *errorAddRouterQDB) AddRouter(
	_ context.Context,
	_ *qdb.Router,
) error {
	return db.addRouterErr
}

func TestClusteredCoordinatorAddRouterError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("failed to add router")

	memDB, err := qdb.NewMemQDB("")
	require.NoError(t, err)

	db := &errorAddRouterQDB{
		XQDB:         memDB,
		addRouterErr: expectedErr,
	}

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	require.NoError(t, err)

	router := &topology.Router{
		ID:      "router-1",
		Address: "127.0.0.1:6432",
	}

	err = qc.AddRouter(ctx, router)

	require.ErrorIs(t, err, expectedErr)
}
