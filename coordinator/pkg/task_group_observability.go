package coord

import (
	"context"
	"fmt"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/qdb"
)

const (
	stageWaiting            = "WAITING"
	stageError              = "ERROR"
	stagePreparingNextMove  = "PREPARING_NEXT_MOVE"
	stageMetadataRename     = "METADATA_RENAME"
	stageSplitKeyRange      = "SPLIT_KEY_RANGE"
	stageMoveData           = "MOVE_DATA"
	stageFinalizeBatch      = "FINALIZE_BATCH"
	stageWholeRangeMetadata = "WHOLE_RANGE_METADATA"
)

func (qc *ClusteredCoordinator) publishTaskGroupObservability(
	ctx context.Context,
	tg *tasks.MoveTaskGroup,
	task *tasks.MoveTask,
	addr string,
	state tasks.TaskGroupState,
	msg string,
) error {
	if tg == nil {
		return nil
	}
	st := qc.buildTaskGroupStatus(ctx, tg, task, addr, state, msg)
	return qc.QDB().WriteTaskGroupStatus(ctx, tg.ID, st)
}

func (qc *ClusteredCoordinator) buildTaskGroupStatus(
	ctx context.Context,
	tg *tasks.MoveTaskGroup,
	task *tasks.MoveTask,
	addr string,
	state tasks.TaskGroupState,
	msg string,
) *qdb.TaskGroupStatus {
	now := time.Now()
	st := &qdb.TaskGroupStatus{
		State:         string(state),
		Message:       msg,
		UpdatedAt:     now,
		KeysProcessed: tg.TotalKeys,
	}
	switch state {
	case tasks.TaskGroupError:
		st.Stage = stageError
		st.Detail = msg
		return st
	case tasks.TaskGroupPlanned:
		st.Stage = stageWaiting
		st.Detail = "queued"
	case tasks.TaskGroupRunning:
		st.Stage = runningStage(task, tg)
		st.Detail = runningDetail(task, tg, addr)
		if tg.Limit > 0 {
			p := float64(tg.TotalKeys) / float64(tg.Limit) * 100.0
			if p > 100 {
				p = 100
			}
			st.ProgressPercent = fmt.Sprintf("%.1f", p)
		}
		bounds, ind, err := qc.GetMoveTaskGroupBoundsCache(ctx, tg.ID)
		if err == nil && len(bounds) > 0 {
			cur := ind
			if cur < 1 {
				cur = 1
			}
			if cur > len(bounds) {
				cur = len(bounds)
			}
			st.BatchPosition = fmt.Sprintf("%d/%d", cur, len(bounds))
		}
		if task != nil {
			st.MoveTaskState = tasks.TaskStateToStr(task.State)
		}
	}
	return st
}

func runningStage(task *tasks.MoveTask, tg *tasks.MoveTaskGroup) string {
	if task == nil {
		return stagePreparingNextMove
	}
	switch task.State {
	case tasks.TaskPlanned:
		if task.Bound == nil && tg != nil && tg.KridTo == task.KridTemp {
			return stageMetadataRename
		}
		if task.Bound == nil {
			return stageWholeRangeMetadata
		}
		return stageSplitKeyRange
	case tasks.TaskSplit:
		return stageMoveData
	case tasks.TaskMoved:
		return stageFinalizeBatch
	default:
		return stagePreparingNextMove
	}
}

func runningDetail(task *tasks.MoveTask, tg *tasks.MoveTaskGroup, addr string) string {
	if tg == nil {
		return ""
	}
	base := fmt.Sprintf("executed by %q", addr)
	if task == nil {
		return base + "; fetching next move batch"
	}
	switch task.State {
	case tasks.TaskPlanned:
		if task.Bound == nil && tg.KridTo == task.KridTemp {
			return base + "; renaming key range metadata"
		}
		if task.Bound == nil {
			return base + "; moving whole key range without split"
		}
		return base + "; applying SPLIT KEY RANGE on routers/coordinator"
	case tasks.TaskSplit:
		return base + "; copying/deleting rows between shards"
	case tasks.TaskMoved:
		return base + "; uniting temporary key ranges / cleanup"
	default:
		return base
	}
}
