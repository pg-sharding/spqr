package tasks

import "context"

type TaskMgr interface {
	GetTaskGroup(ctx context.Context) (*MoveTaskGroup, error)
	WriteTaskGroup(ctx context.Context, taskGroup *MoveTaskGroup) error
	RemoveTaskGroup(ctx context.Context) error

	GetBalancerTask(ctx context.Context) (*BalancerTask, error)
	WriteBalancerTask(ctx context.Context, task *BalancerTask) error
	RemoveBalancerTask(ctx context.Context) error
}
