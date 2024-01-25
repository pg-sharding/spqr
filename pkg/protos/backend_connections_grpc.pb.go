// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.20.0
// source: protos/backend_connections.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// BackendConnectionsServiceClient is the client API for BackendConnectionsService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type BackendConnectionsServiceClient interface {
	ListBackendConnections(ctx context.Context, in *ListBackendConnectionsRequest, opts ...grpc.CallOption) (*ListBackendConntionsReply, error)
}

type backendConnectionsServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewBackendConnectionsServiceClient(cc grpc.ClientConnInterface) BackendConnectionsServiceClient {
	return &backendConnectionsServiceClient{cc}
}

func (c *backendConnectionsServiceClient) ListBackendConnections(ctx context.Context, in *ListBackendConnectionsRequest, opts ...grpc.CallOption) (*ListBackendConntionsReply, error) {
	out := new(ListBackendConntionsReply)
	err := c.cc.Invoke(ctx, "/spqr.BackendConnectionsService/ListBackendConnections", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BackendConnectionsServiceServer is the server API for BackendConnectionsService service.
// All implementations must embed UnimplementedBackendConnectionsServiceServer
// for forward compatibility
type BackendConnectionsServiceServer interface {
	ListBackendConnections(context.Context, *ListBackendConnectionsRequest) (*ListBackendConntionsReply, error)
	mustEmbedUnimplementedBackendConnectionsServiceServer()
}

// UnimplementedBackendConnectionsServiceServer must be embedded to have forward compatible implementations.
type UnimplementedBackendConnectionsServiceServer struct {
}

func (UnimplementedBackendConnectionsServiceServer) ListBackendConnections(context.Context, *ListBackendConnectionsRequest) (*ListBackendConntionsReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListBackendConnections not implemented")
}
func (UnimplementedBackendConnectionsServiceServer) mustEmbedUnimplementedBackendConnectionsServiceServer() {
}

// UnsafeBackendConnectionsServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to BackendConnectionsServiceServer will
// result in compilation errors.
type UnsafeBackendConnectionsServiceServer interface {
	mustEmbedUnimplementedBackendConnectionsServiceServer()
}

func RegisterBackendConnectionsServiceServer(s grpc.ServiceRegistrar, srv BackendConnectionsServiceServer) {
	s.RegisterService(&BackendConnectionsService_ServiceDesc, srv)
}

func _BackendConnectionsService_ListBackendConnections_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListBackendConnectionsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BackendConnectionsServiceServer).ListBackendConnections(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/spqr.BackendConnectionsService/ListBackendConnections",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BackendConnectionsServiceServer).ListBackendConnections(ctx, req.(*ListBackendConnectionsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// BackendConnectionsService_ServiceDesc is the grpc.ServiceDesc for BackendConnectionsService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var BackendConnectionsService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "spqr.BackendConnectionsService",
	HandlerType: (*BackendConnectionsServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ListBackendConnections",
			Handler:    _BackendConnectionsService_ListBackendConnections_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "protos/backend_connections.proto",
}
