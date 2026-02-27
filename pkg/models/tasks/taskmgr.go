package tasks

import "context"

type TaskMgr interface {
	ListMoveTaskGroups(ctx context.Context) (map[string]*MoveTaskGroup, error)
	GetMoveTaskGroup(ctx context.Context, id string) (*MoveTaskGroup, error)
	WriteMoveTaskGroup(ctx context.Context, taskGroup *MoveTaskGroup) error
	DropMoveTaskGroup(ctx context.Context, id string, cascade bool) error
	RetryMoveTaskGroup(ctx context.Context, id string) error
	StopMoveTaskGroup(ctx context.Context, id string) error
	GetMoveTaskGroupBoundsCache(ctx context.Context, id string) ([][][]byte, int, error)

	GetTaskGroupStatus(ctx context.Context, id string) (*MoveTaskGroupStatus, error)
	GetAllTaskGroupStatuses(ctx context.Context) (map[string]*MoveTaskGroupStatus, error)

	ListMoveTasks(ctx context.Context) (map[string]*MoveTask, error)
	GetMoveTask(ctx context.Context, id string) (*MoveTask, error)
	DropMoveTask(ctx context.Context, id string) error

	ListRedistributeTasks(ctx context.Context) ([]*RedistributeTask, error)
	DropRedistributeTask(ctx context.Context, id string, cascade bool) error

	GetBalancerTask(ctx context.Context) (*BalancerTask, error)
	WriteBalancerTask(ctx context.Context, task *BalancerTask) error
	DropBalancerTask(ctx context.Context) error
}
