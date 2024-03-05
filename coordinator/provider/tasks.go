package provider

import (
	"context"
	"github.com/pg-sharding/spqr/coordinator"
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

func (t TasksServer) GetTaskGroup(ctx context.Context, request *protos.GetTaskGroupRequest) (*protos.GetTaskGroupReply, error) {
	//TODO implement me
	panic("implement me")
}

func (t TasksServer) WriteTaskGroup(ctx context.Context, request *protos.WriteTaskGroupRequest) (*protos.WriteTaskGroupReply, error) {
	//TODO implement me
	panic("implement me")
}

func (t TasksServer) RemoveTaskGroup(ctx context.Context, request *protos.RemoveTaskGroupRequest) (*protos.RemoveTaskGroupReply, error) {
	//TODO implement me
	panic("implement me")
}

func (t TasksServer) mustEmbedUnimplementedTasksServiceServer() {
	//TODO implement me
	panic("implement me")
}
