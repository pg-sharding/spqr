package tasks

import "context"

type TaskMgr interface {
	GetMoveTaskGroup(ctx context.Context) (*MoveTaskGroup, error)
	WriteMoveTaskGroup(ctx context.Context, taskGroup *MoveTaskGroup) error
	RemoveMoveTaskGroup(ctx context.Context) error
	RetryMoveTaskGroup(ctx context.Context) error
	StopMoveTaskGroup(ctx context.Context) error

	GetBalancerTask(ctx context.Context) (*BalancerTask, error)
	WriteBalancerTask(ctx context.Context, task *BalancerTask) error
	RemoveBalancerTask(ctx context.Context) error
}
