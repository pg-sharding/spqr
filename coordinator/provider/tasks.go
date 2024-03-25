package provider

import (
	"context"
	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	protos "github.com/pg-sharding/spqr/pkg/protos"
)

type TasksServer struct {
	protos.UnimplementedTasksServiceServer

	impl coordinator.Coordinator
}

func NewTasksServer(impl coordinator.Coordinator) *TasksServer {
	return &TasksServer{
		impl: impl,
	}
}

var _ protos.TasksServiceServer = &TasksServer{}

func (t TasksServer) GetTaskGroup(ctx context.Context, _ *protos.GetTaskGroupRequest) (*protos.GetTaskGroupReply, error) {
	group, err := t.impl.GetTaskGroup(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.GetTaskGroupReply{TaskGroup: tasks.TaskGroupToProto(group)}, nil
}

func (t TasksServer) WriteTaskGroup(ctx context.Context, request *protos.WriteTaskGroupRequest) (*protos.WriteTaskGroupReply, error) {
	err := t.impl.WriteTaskGroup(ctx, tasks.TaskGroupFromProto(request.TaskGroup))
	return &protos.WriteTaskGroupReply{}, err
}

func (t TasksServer) RemoveTaskGroup(ctx context.Context, _ *protos.RemoveTaskGroupRequest) (*protos.RemoveTaskGroupReply, error) {
	return &protos.RemoveTaskGroupReply{}, t.impl.RemoveTaskGroup(ctx)
}
