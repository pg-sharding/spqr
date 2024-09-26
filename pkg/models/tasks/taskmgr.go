package tasks

import "context"

type TaskMgr interface {
	GetTaskGroup(ctx context.Context) (*MoveTaskGroup, error)
	WriteTaskGroup(ctx context.Context, taskGroup *MoveTaskGroup) error
	RemoveTaskGroup(ctx context.Context) error
}
