package tasks

import "context"

type TaskMgr interface {
	GetTaskGroup(ctx context.Context) (*TaskGroup, error)
	WriteTaskGroup(ctx context.Context, taskGroup *TaskGroup) error
	RemoveTaskGroup(ctx context.Context) error
}
