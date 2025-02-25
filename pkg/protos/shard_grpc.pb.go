// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v3.21.12
// source: protos/shard.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	ShardService_ListShards_FullMethodName    = "/spqr.ShardService/ListShards"
	ShardService_AddDataShard_FullMethodName  = "/spqr.ShardService/AddDataShard"
	ShardService_AddWorldShard_FullMethodName = "/spqr.ShardService/AddWorldShard"
	ShardService_GetShard_FullMethodName      = "/spqr.ShardService/GetShard"
)

// ShardServiceClient is the client API for ShardService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ShardServiceClient interface {
	ListShards(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListShardsReply, error)
	AddDataShard(ctx context.Context, in *AddShardRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	AddWorldShard(ctx context.Context, in *AddWorldShardRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	GetShard(ctx context.Context, in *ShardRequest, opts ...grpc.CallOption) (*ShardReply, error)
}

type shardServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewShardServiceClient(cc grpc.ClientConnInterface) ShardServiceClient {
	return &shardServiceClient{cc}
}

func (c *shardServiceClient) ListShards(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListShardsReply, error) {
	out := new(ListShardsReply)
	err := c.cc.Invoke(ctx, ShardService_ListShards_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *shardServiceClient) AddDataShard(ctx context.Context, in *AddShardRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, ShardService_AddDataShard_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *shardServiceClient) AddWorldShard(ctx context.Context, in *AddWorldShardRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, ShardService_AddWorldShard_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *shardServiceClient) GetShard(ctx context.Context, in *ShardRequest, opts ...grpc.CallOption) (*ShardReply, error) {
	out := new(ShardReply)
	err := c.cc.Invoke(ctx, ShardService_GetShard_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ShardServiceServer is the server API for ShardService service.
// All implementations must embed UnimplementedShardServiceServer
// for forward compatibility
type ShardServiceServer interface {
	ListShards(context.Context, *emptypb.Empty) (*ListShardsReply, error)
	AddDataShard(context.Context, *AddShardRequest) (*emptypb.Empty, error)
	AddWorldShard(context.Context, *AddWorldShardRequest) (*emptypb.Empty, error)
	GetShard(context.Context, *ShardRequest) (*ShardReply, error)
	mustEmbedUnimplementedShardServiceServer()
}

// UnimplementedShardServiceServer must be embedded to have forward compatible implementations.
type UnimplementedShardServiceServer struct {
}

func (UnimplementedShardServiceServer) ListShards(context.Context, *emptypb.Empty) (*ListShardsReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListShards not implemented")
}
func (UnimplementedShardServiceServer) AddDataShard(context.Context, *AddShardRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddDataShard not implemented")
}
func (UnimplementedShardServiceServer) AddWorldShard(context.Context, *AddWorldShardRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddWorldShard not implemented")
}
func (UnimplementedShardServiceServer) GetShard(context.Context, *ShardRequest) (*ShardReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetShard not implemented")
}
func (UnimplementedShardServiceServer) mustEmbedUnimplementedShardServiceServer() {}

// UnsafeShardServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ShardServiceServer will
// result in compilation errors.
type UnsafeShardServiceServer interface {
	mustEmbedUnimplementedShardServiceServer()
}

func RegisterShardServiceServer(s grpc.ServiceRegistrar, srv ShardServiceServer) {
	s.RegisterService(&ShardService_ServiceDesc, srv)
}

func _ShardService_ListShards_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ShardServiceServer).ListShards(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ShardService_ListShards_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ShardServiceServer).ListShards(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _ShardService_AddDataShard_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddShardRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ShardServiceServer).AddDataShard(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ShardService_AddDataShard_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ShardServiceServer).AddDataShard(ctx, req.(*AddShardRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ShardService_AddWorldShard_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddWorldShardRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ShardServiceServer).AddWorldShard(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ShardService_AddWorldShard_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ShardServiceServer).AddWorldShard(ctx, req.(*AddWorldShardRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ShardService_GetShard_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ShardRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ShardServiceServer).GetShard(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ShardService_GetShard_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ShardServiceServer).GetShard(ctx, req.(*ShardRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ShardService_ServiceDesc is the grpc.ServiceDesc for ShardService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ShardService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "spqr.ShardService",
	HandlerType: (*ShardServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ListShards",
			Handler:    _ShardService_ListShards_Handler,
		},
		{
			MethodName: "AddDataShard",
			Handler:    _ShardService_AddDataShard_Handler,
		},
		{
			MethodName: "AddWorldShard",
			Handler:    _ShardService_AddWorldShard_Handler,
		},
		{
			MethodName: "GetShard",
			Handler:    _ShardService_GetShard_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "protos/shard.proto",
}
