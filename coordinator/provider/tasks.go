package provider

import (
	"context"
	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	protos "github.com/pg-sharding/spqr/pkg/protos"
)

type TasksServer struct {
	protos.UnimplementedMoveTasksServiceServer
	protos.UnimplementedBalancerTaskServiceServer

	impl coordinator.Coordinator
}

func NewTasksServer(impl coordinator.Coordinator) *TasksServer {
	return &TasksServer{
		impl: impl,
	}
}

var _ protos.MoveTasksServiceServer = &TasksServer{}
var _ protos.BalancerTaskServiceServer = &TasksServer{}

func (t TasksServer) GetMoveTaskGroup(ctx context.Context, _ *protos.GetMoveTaskGroupRequest) (*protos.GetMoveTaskGroupReply, error) {
	group, err := t.impl.GetMoveTaskGroup(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.GetMoveTaskGroupReply{TaskGroup: tasks.TaskGroupToProto(group)}, nil
}

func (t TasksServer) WriteMoveTaskGroup(ctx context.Context, request *protos.WriteMoveTaskGroupRequest) (*protos.WriteMoveTaskGroupReply, error) {
	err := t.impl.WriteMoveTaskGroup(ctx, tasks.TaskGroupFromProto(request.TaskGroup))
	return &protos.WriteMoveTaskGroupReply{}, err
}

func (t TasksServer) RemoveMoveTaskGroup(ctx context.Context, _ *protos.RemoveMoveTaskGroupRequest) (*protos.RemoveMoveTaskGroupReply, error) {
	return &protos.RemoveMoveTaskGroupReply{}, t.impl.RemoveMoveTaskGroup(ctx)
}

func (t TasksServer) GetBalancerTask(ctx context.Context, _ *protos.GetBalancerTaskRequest) (*protos.GetBalancerTaskReply, error) {
	task, err := t.impl.GetBalancerTask(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.GetBalancerTaskReply{Task: tasks.BalancerTaskToProto(task)}, nil
}

func (t TasksServer) WriteBalancerTask(ctx context.Context, request *protos.WriteBalancerTaskRequest) (*protos.WriteBalancerTaskReply, error) {
	return &protos.WriteBalancerTaskReply{}, t.impl.WriteBalancerTask(ctx, tasks.BalancerTaskFromProto(request.Task))
}

func (t TasksServer) RemoveBalancerTask(ctx context.Context, _ *protos.RemoveBalancerTaskRequest) (*protos.RemoveBalancerTaskReply, error) {
	return &protos.RemoveBalancerTaskReply{}, t.impl.RemoveBalancerTask(ctx)
}
