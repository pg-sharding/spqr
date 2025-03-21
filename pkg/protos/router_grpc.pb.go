// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.28.0
// source: protos/router.proto

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
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	RouterService_ListRouters_FullMethodName  = "/spqr.RouterService/ListRouters"
	RouterService_AddRouter_FullMethodName    = "/spqr.RouterService/AddRouter"
	RouterService_RemoveRouter_FullMethodName = "/spqr.RouterService/RemoveRouter"
	RouterService_SyncMetadata_FullMethodName = "/spqr.RouterService/SyncMetadata"
)

// RouterServiceClient is the client API for RouterService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type RouterServiceClient interface {
	ListRouters(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListRoutersReply, error)
	AddRouter(ctx context.Context, in *AddRouterRequest, opts ...grpc.CallOption) (*AddRouterReply, error)
	RemoveRouter(ctx context.Context, in *RemoveRouterRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	SyncMetadata(ctx context.Context, in *SyncMetadataRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type routerServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewRouterServiceClient(cc grpc.ClientConnInterface) RouterServiceClient {
	return &routerServiceClient{cc}
}

func (c *routerServiceClient) ListRouters(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListRoutersReply, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListRoutersReply)
	err := c.cc.Invoke(ctx, RouterService_ListRouters_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *routerServiceClient) AddRouter(ctx context.Context, in *AddRouterRequest, opts ...grpc.CallOption) (*AddRouterReply, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(AddRouterReply)
	err := c.cc.Invoke(ctx, RouterService_AddRouter_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *routerServiceClient) RemoveRouter(ctx context.Context, in *RemoveRouterRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, RouterService_RemoveRouter_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *routerServiceClient) SyncMetadata(ctx context.Context, in *SyncMetadataRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, RouterService_SyncMetadata_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RouterServiceServer is the server API for RouterService service.
// All implementations must embed UnimplementedRouterServiceServer
// for forward compatibility.
type RouterServiceServer interface {
	ListRouters(context.Context, *emptypb.Empty) (*ListRoutersReply, error)
	AddRouter(context.Context, *AddRouterRequest) (*AddRouterReply, error)
	RemoveRouter(context.Context, *RemoveRouterRequest) (*emptypb.Empty, error)
	SyncMetadata(context.Context, *SyncMetadataRequest) (*emptypb.Empty, error)
	mustEmbedUnimplementedRouterServiceServer()
}

// UnimplementedRouterServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedRouterServiceServer struct{}

func (UnimplementedRouterServiceServer) ListRouters(context.Context, *emptypb.Empty) (*ListRoutersReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListRouters not implemented")
}
func (UnimplementedRouterServiceServer) AddRouter(context.Context, *AddRouterRequest) (*AddRouterReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddRouter not implemented")
}
func (UnimplementedRouterServiceServer) RemoveRouter(context.Context, *RemoveRouterRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RemoveRouter not implemented")
}
func (UnimplementedRouterServiceServer) SyncMetadata(context.Context, *SyncMetadataRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SyncMetadata not implemented")
}
func (UnimplementedRouterServiceServer) mustEmbedUnimplementedRouterServiceServer() {}
func (UnimplementedRouterServiceServer) testEmbeddedByValue()                       {}

// UnsafeRouterServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RouterServiceServer will
// result in compilation errors.
type UnsafeRouterServiceServer interface {
	mustEmbedUnimplementedRouterServiceServer()
}

func RegisterRouterServiceServer(s grpc.ServiceRegistrar, srv RouterServiceServer) {
	// If the following call pancis, it indicates UnimplementedRouterServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&RouterService_ServiceDesc, srv)
}

func _RouterService_ListRouters_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RouterServiceServer).ListRouters(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RouterService_ListRouters_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RouterServiceServer).ListRouters(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _RouterService_AddRouter_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddRouterRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RouterServiceServer).AddRouter(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RouterService_AddRouter_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RouterServiceServer).AddRouter(ctx, req.(*AddRouterRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RouterService_RemoveRouter_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RemoveRouterRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RouterServiceServer).RemoveRouter(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RouterService_RemoveRouter_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RouterServiceServer).RemoveRouter(ctx, req.(*RemoveRouterRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RouterService_SyncMetadata_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SyncMetadataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RouterServiceServer).SyncMetadata(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RouterService_SyncMetadata_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RouterServiceServer).SyncMetadata(ctx, req.(*SyncMetadataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// RouterService_ServiceDesc is the grpc.ServiceDesc for RouterService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var RouterService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "spqr.RouterService",
	HandlerType: (*RouterServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ListRouters",
			Handler:    _RouterService_ListRouters_Handler,
		},
		{
			MethodName: "AddRouter",
			Handler:    _RouterService_AddRouter_Handler,
		},
		{
			MethodName: "RemoveRouter",
			Handler:    _RouterService_RemoveRouter_Handler,
		},
		{
			MethodName: "SyncMetadata",
			Handler:    _RouterService_SyncMetadata_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "protos/router.proto",
}
