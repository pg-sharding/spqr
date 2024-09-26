package provider

import (
	"context"
	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	protos "github.com/pg-sharding/spqr/pkg/protos"
)

type TasksServer struct {
	protos.UnimplementedMoveTasksServiceServer

	impl coordinator.Coordinator
}

func NewTasksServer(impl coordinator.Coordinator) *TasksServer {
	return &TasksServer{
		impl: impl,
	}
}

var _ protos.MoveTasksServiceServer = &TasksServer{}

func (t TasksServer) GetMoveTaskGroup(ctx context.Context, _ *protos.GetMoveTaskGroupRequest) (*protos.GetMoveTaskGroupReply, error) {
	group, err := t.impl.GetTaskGroup(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.GetMoveTaskGroupReply{TaskGroup: tasks.TaskGroupToProto(group)}, nil
}

func (t TasksServer) WriteMoveTaskGroup(ctx context.Context, request *protos.WriteMoveTaskGroupRequest) (*protos.WriteMoveTaskGroupReply, error) {
	err := t.impl.WriteTaskGroup(ctx, tasks.TaskGroupFromProto(request.TaskGroup))
	return &protos.WriteMoveTaskGroupReply{}, err
}

func (t TasksServer) RemoveMoveTaskGroup(ctx context.Context, _ *protos.RemoveMoveTaskGroupRequest) (*protos.RemoveMoveTaskGroupReply, error) {
	return &protos.RemoveMoveTaskGroupReply{}, t.impl.RemoveTaskGroup(ctx)
}
