package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"google.golang.org/protobuf/types/known/emptypb"
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

func (t *TasksServer) ListMoveTasks(ctx context.Context, _ *emptypb.Empty) (*protos.MoveTasksReply, error) {
	taskList, err := t.impl.ListMoveTasks(ctx)
	if err != nil {
		return nil, err
	}
	tasksProto := make([]*protos.MoveTask, 0, len(taskList))
	for _, taskProto := range taskList {
		tasksProto = append(tasksProto, tasks.MoveTaskToProto(taskProto))
	}
	return &protos.MoveTasksReply{Tasks: tasksProto}, nil
}

func (t *TasksServer) ListMoveTaskGroups(ctx context.Context, _ *emptypb.Empty) (*protos.ListMoveTaskGroupsReply, error) {
	groups, err := t.impl.ListMoveTaskGroups(ctx)
	if err != nil {
		return nil, err
	}
	taskGroupsProto := make([]*protos.MoveTaskGroup, 0, len(groups))
	for _, groupProto := range groups {
		taskGroupsProto = append(taskGroupsProto, tasks.TaskGroupToProto(groupProto))
	}
	return &protos.ListMoveTaskGroupsReply{TaskGroups: taskGroupsProto}, nil
}

func (t TasksServer) GetMoveTaskGroup(ctx context.Context, req *protos.MoveTaskGroupSelector) (*protos.GetMoveTaskGroupReply, error) {
	group, err := t.impl.GetMoveTaskGroup(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	return &protos.GetMoveTaskGroupReply{TaskGroup: tasks.TaskGroupToProto(group)}, nil
}

func (t TasksServer) WriteMoveTaskGroup(ctx context.Context, request *protos.WriteMoveTaskGroupRequest) (*emptypb.Empty, error) {
	err := t.impl.WriteMoveTaskGroup(ctx, tasks.TaskGroupFromProto(request.TaskGroup))
	return nil, err
}

func (t TasksServer) RemoveMoveTaskGroup(ctx context.Context, req *protos.MoveTaskGroupSelector) (*emptypb.Empty, error) {
	return nil, t.impl.RemoveMoveTaskGroup(ctx, req.ID)
}

func (t TasksServer) RetryMoveTaskGroup(ctx context.Context, req *protos.MoveTaskGroupSelector) (*emptypb.Empty, error) {
	return nil, t.impl.RetryMoveTaskGroup(ctx, req.ID)
}

func (t TasksServer) StopMoveTaskGroup(ctx context.Context, req *protos.MoveTaskGroupSelector) (*emptypb.Empty, error) {
	return nil, t.impl.StopMoveTaskGroup(ctx, req.ID)
}

func (t TasksServer) GetBalancerTask(ctx context.Context, _ *emptypb.Empty) (*protos.GetBalancerTaskReply, error) {
	task, err := t.impl.GetBalancerTask(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.GetBalancerTaskReply{Task: tasks.BalancerTaskToProto(task)}, nil
}

func (t TasksServer) WriteBalancerTask(ctx context.Context, request *protos.WriteBalancerTaskRequest) (*emptypb.Empty, error) {
	return nil, t.impl.WriteBalancerTask(ctx, tasks.BalancerTaskFromProto(request.Task))
}

func (t TasksServer) RemoveBalancerTask(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, t.impl.RemoveBalancerTask(ctx)
}
