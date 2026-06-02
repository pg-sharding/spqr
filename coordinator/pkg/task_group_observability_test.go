package coord

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

func TestBuildTaskGroupStatusError(t *testing.T) {
	db, err := qdb.NewMemQDB("")
	assert.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	assert.NoError(t, err)

	tg := &tasks.MoveTaskGroup{
		ID:        "tg-error",
		TotalKeys: 100,
	}

	status := qc.buildTaskGroupStatus(context.Background(), tg, nil, "", tasks.TaskGroupError, "test error")

	assert.Equal(t, string(tasks.TaskGroupError), status.State)
	assert.Equal(t, "test error", status.Message)
	assert.Equal(t, stageError, status.Stage)
	assert.Equal(t, "test error", status.Detail)
	assert.Equal(t, int64(100), status.KeysProcessed)
	assert.False(t, status.UpdatedAt.IsZero())
}

func TestBuildTaskGroupStatusPlanned(t *testing.T) {
	db, err := qdb.NewMemQDB("")
	assert.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	assert.NoError(t, err)

	tg := &tasks.MoveTaskGroup{
		ID:        "tg-planned",
		TotalKeys: 50,
	}

	status := qc.buildTaskGroupStatus(context.Background(), tg, nil, "", tasks.TaskGroupPlanned, "")

	assert.Equal(t, string(tasks.TaskGroupPlanned), status.State)
	assert.Equal(t, stageWaiting, status.Stage)
	assert.Equal(t, "queued", status.Detail)
	assert.Equal(t, int64(50), status.KeysProcessed)
	assert.False(t, status.UpdatedAt.IsZero())
}

func TestBuildTaskGroupStatusRunningNoTaskNoProgress(t *testing.T) {
	db, err := qdb.NewMemQDB("")
	assert.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	assert.NoError(t, err)

	tg := &tasks.MoveTaskGroup{
		ID:        "tg-running",
		TotalKeys: 0,
	}

	status := qc.buildTaskGroupStatus(context.Background(), tg, nil, "localhost:5432", tasks.TaskGroupRunning, "running")

	assert.Equal(t, string(tasks.TaskGroupRunning), status.State)
	assert.Equal(t, stagePreparingNextMove, status.Stage)
	assert.Equal(t, `executed by "localhost:5432"; fetching next move batch`, status.Detail)
	assert.Equal(t, int64(0), status.KeysProcessed)
	assert.Empty(t, status.ProgressPercent)
	assert.False(t, status.UpdatedAt.IsZero())
}

func TestBuildTaskGroupStatusRunningWithProgressPercent(t *testing.T) {
	db, err := qdb.NewMemQDB("")
	assert.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	assert.NoError(t, err)

	tg := &tasks.MoveTaskGroup{
		ID:        "tg-running",
		TotalKeys: 50,
		Limit:     100,
	}

	status := qc.buildTaskGroupStatus(context.Background(), tg, nil, "localhost:5432", tasks.TaskGroupRunning, "running")

	assert.Equal(t, "50.0", status.ProgressPercent)
}

func TestBuildTaskGroupStatusRunningProgressExceeds100Percent(t *testing.T) {
	db, err := qdb.NewMemQDB("")
	assert.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	assert.NoError(t, err)

	tg := &tasks.MoveTaskGroup{
		ID:        "tg-running",
		TotalKeys: 150,
		Limit:     100,
	}

	status := qc.buildTaskGroupStatus(context.Background(), tg, nil, "localhost:5432", tasks.TaskGroupRunning, "running")

	assert.Equal(t, "100.0", status.ProgressPercent)
}

func TestRunningStageTaskPlannedWithMetadataRename(t *testing.T) {
	task := &tasks.MoveTask{
		State:    tasks.TaskPlanned,
		Bound:    nil,
		KridTemp: "kr-temp",
	}
	tg := &tasks.MoveTaskGroup{
		KridTo: "kr-temp",
	}

	stage := runningStage(task, tg)
	assert.Equal(t, stageMetadataRename, stage)
}

func TestRunningStageTaskPlannedWholeRange(t *testing.T) {
	task := &tasks.MoveTask{
		State:    tasks.TaskPlanned,
		Bound:    nil,
		KridTemp: "kr-temp-1",
	}
	tg := &tasks.MoveTaskGroup{
		KridTo: "kr-temp-2",
	}

	stage := runningStage(task, tg)
	assert.Equal(t, stageWholeRangeMetadata, stage)
}

func TestRunningStageTaskPlannedSplit(t *testing.T) {
	task := &tasks.MoveTask{
		State: tasks.TaskPlanned,
		Bound: [][]byte{{1, 2, 3}},
	}
	tg := &tasks.MoveTaskGroup{
		KridTo: "kr-temp",
	}

	stage := runningStage(task, tg)
	assert.Equal(t, stageSplitKeyRange, stage)
}

func TestRunningStageTaskSplit(t *testing.T) {
	task := &tasks.MoveTask{
		State: tasks.TaskSplit,
	}

	stage := runningStage(task, nil)
	assert.Equal(t, stageMoveData, stage)
}

func TestRunningStageTaskMoved(t *testing.T) {
	task := &tasks.MoveTask{
		State: tasks.TaskMoved,
	}

	stage := runningStage(task, nil)
	assert.Equal(t, stageFinalizeBatch, stage)
}

func TestRunningStageNoTask(t *testing.T) {
	stage := runningStage(nil, &tasks.MoveTaskGroup{})
	assert.Equal(t, stagePreparingNextMove, stage)
}

func TestRunningDetailTaskPlannedMetadataRename(t *testing.T) {
	task := &tasks.MoveTask{
		State:    tasks.TaskPlanned,
		Bound:    nil,
		KridTemp: "kr-temp",
	}
	tg := &tasks.MoveTaskGroup{
		KridTo: "kr-temp",
	}

	detail := runningDetail(task, tg, "localhost:5432")
	assert.Equal(t, `executed by "localhost:5432"; renaming key range metadata`, detail)
}

func TestRunningDetailTaskPlannedWholeRange(t *testing.T) {
	task := &tasks.MoveTask{
		State:    tasks.TaskPlanned,
		Bound:    nil,
		KridTemp: "kr-temp-1",
	}
	tg := &tasks.MoveTaskGroup{
		KridTo: "kr-temp-2",
	}

	detail := runningDetail(task, tg, "localhost:5432")
	assert.Equal(t, `executed by "localhost:5432"; moving whole key range without split`, detail)
}

func TestRunningDetailTaskPlannedSplit(t *testing.T) {
	task := &tasks.MoveTask{
		State: tasks.TaskPlanned,
		Bound: [][]byte{{1, 2, 3}},
	}
	tg := &tasks.MoveTaskGroup{}

	detail := runningDetail(task, tg, "localhost:5432")
	assert.Equal(t, `executed by "localhost:5432"; applying SPLIT KEY RANGE on routers/coordinator`, detail)
}

func TestRunningDetailTaskSplit(t *testing.T) {
	task := &tasks.MoveTask{
		State: tasks.TaskSplit,
	}
	tg := &tasks.MoveTaskGroup{}

	detail := runningDetail(task, tg, "localhost:5432")
	assert.Equal(t, `executed by "localhost:5432"; copying/deleting rows between shards`, detail)
}

func TestRunningDetailTaskMoved(t *testing.T) {
	task := &tasks.MoveTask{
		State: tasks.TaskMoved,
	}
	tg := &tasks.MoveTaskGroup{}

	detail := runningDetail(task, tg, "localhost:5432")
	assert.Equal(t, `executed by "localhost:5432"; uniting temporary key ranges / cleanup`, detail)
}

func TestRunningDetailNoTask(t *testing.T) {
	tg := &tasks.MoveTaskGroup{}

	detail := runningDetail(nil, tg, "localhost:5432")
	assert.Equal(t, `executed by "localhost:5432"; fetching next move batch`, detail)
}

func TestRunningDetailNilTaskGroup(t *testing.T) {
	detail := runningDetail(nil, nil, "localhost:5432")
	assert.Equal(t, "", detail)
}

func TestPublishTaskGroupObservabilityNilTaskGroup(t *testing.T) {
	db, err := qdb.NewMemQDB("")
	assert.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	assert.NoError(t, err)

	err = qc.publishTaskGroupObservability(context.Background(), nil, nil, "", tasks.TaskGroupPlanned, "")
	assert.NoError(t, err)
}

func TestPublishTaskGroupObservabilitySuccess(t *testing.T) {
	db, err := qdb.NewMemQDB("")
	assert.NoError(t, err)

	qc, err := NewClusteredCoordinator(nil, db, qdb.DefaultMaxTxnSize)
	assert.NoError(t, err)

	tg := &tasks.MoveTaskGroup{
		ID:        "tg-pub",
		TotalKeys: 75,
	}

	err = qc.publishTaskGroupObservability(context.Background(), tg, nil, "localhost:5432", tasks.TaskGroupPlanned, "")
	assert.NoError(t, err)

	status, err := db.GetTaskGroupStatus(context.Background(), tg.ID)
	assert.NoError(t, err)
	assert.NotNil(t, status)
	assert.Equal(t, string(tasks.TaskGroupPlanned), status.State)
	assert.Equal(t, stageWaiting, status.Stage)
	assert.Equal(t, int64(75), status.KeysProcessed)
}
