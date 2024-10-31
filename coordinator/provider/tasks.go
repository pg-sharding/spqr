package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"google.golang.org/protobuf/types/known/emptypb"
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

func (t TasksServer) GetTaskGroup(ctx context.Context, _ *emptypb.Empty) (*protos.GetTaskGroupReply, error) {
	group, err := t.impl.GetTaskGroup(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.GetTaskGroupReply{TaskGroup: tasks.TaskGroupToProto(group)}, nil
}

func (t TasksServer) WriteTaskGroup(ctx context.Context, request *protos.WriteTaskGroupRequest) (*emptypb.Empty, error) {
	err := t.impl.WriteTaskGroup(ctx, tasks.TaskGroupFromProto(request.TaskGroup))
	return nil, err
}

func (t TasksServer) RemoveTaskGroup(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, t.impl.RemoveTaskGroup(ctx)
}
