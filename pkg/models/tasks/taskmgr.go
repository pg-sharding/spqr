package tasks

import "context"

type TaskMgr interface {
	ListMoveTaskGroups(ctx context.Context) (map[string]*MoveTaskGroup, error)
	GetMoveTaskGroup(ctx context.Context, id string) (*MoveTaskGroup, error)
	WriteMoveTaskGroup(ctx context.Context, taskGroup *MoveTaskGroup) error
	RemoveMoveTaskGroup(ctx context.Context, id string) error
	RetryMoveTaskGroup(ctx context.Context, id string) error
	StopMoveTaskGroup(ctx context.Context, id string) error

	ListMoveTasks(ctx context.Context) (map[string]*MoveTask, error)

	GetBalancerTask(ctx context.Context) (*BalancerTask, error)
	WriteBalancerTask(ctx context.Context, task *BalancerTask) error
	RemoveBalancerTask(ctx context.Context) error
}
