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

	impl coordinator.Coordinator
}

func NewTasksServer(impl coordinator.Coordinator) *TasksServer {
	return &TasksServer{
		impl: impl,
	}
}

var _ protos.MoveTasksServiceServer = &TasksServer{}

func (t TasksServer) GetMoveTaskGroup(ctx context.Context, _ *emptypb.Empty) (*protos.GetMoveTaskGroupReply, error) {
	group, err := t.impl.GetTaskGroup(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.GetMoveTaskGroupReply{TaskGroup: tasks.TaskGroupToProto(group)}, nil
}

func (t TasksServer) WriteMoveTaskGroup(ctx context.Context, request *protos.WriteMoveTaskGroupRequest) (*emptypb.Empty, error) {
	err := t.impl.WriteTaskGroup(ctx, tasks.TaskGroupFromProto(request.TaskGroup))
	return nil, err
}

func (t TasksServer) RemoveMoveTaskGroup(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, t.impl.RemoveTaskGroup(ctx)
}
