// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.6.1
// source: protos/dataspace.proto

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

// DataspaceServiceClient is the client API for DataspaceService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DataspaceServiceClient interface {
	AddDataspace(ctx context.Context, in *AddDataspaceRequest, opts ...grpc.CallOption) (*AddDataspaceReply, error)
	DropDataspace(ctx context.Context, in *DropDataspaceRequest, opts ...grpc.CallOption) (*DropDataspaceReply, error)
	ListDataspace(ctx context.Context, in *ListDataspaceRequest, opts ...grpc.CallOption) (*ListDataspaceReply, error)
}

type dataspaceServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewDataspaceServiceClient(cc grpc.ClientConnInterface) DataspaceServiceClient {
	return &dataspaceServiceClient{cc}
}

func (c *dataspaceServiceClient) AddDataspace(ctx context.Context, in *AddDataspaceRequest, opts ...grpc.CallOption) (*AddDataspaceReply, error) {
	out := new(AddDataspaceReply)
	err := c.cc.Invoke(ctx, "/spqr.DataspaceService/AddDataspace", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dataspaceServiceClient) DropDataspace(ctx context.Context, in *DropDataspaceRequest, opts ...grpc.CallOption) (*DropDataspaceReply, error) {
	out := new(DropDataspaceReply)
	err := c.cc.Invoke(ctx, "/spqr.DataspaceService/DropDataspace", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dataspaceServiceClient) ListDataspace(ctx context.Context, in *ListDataspaceRequest, opts ...grpc.CallOption) (*ListDataspaceReply, error) {
	out := new(ListDataspaceReply)
	err := c.cc.Invoke(ctx, "/spqr.DataspaceService/ListDataspace", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DataspaceServiceServer is the server API for DataspaceService service.
// All implementations must embed UnimplementedDataspaceServiceServer
// for forward compatibility
type DataspaceServiceServer interface {
	AddDataspace(context.Context, *AddDataspaceRequest) (*AddDataspaceReply, error)
	DropDataspace(context.Context, *DropDataspaceRequest) (*DropDataspaceReply, error)
	ListDataspace(context.Context, *ListDataspaceRequest) (*ListDataspaceReply, error)
	mustEmbedUnimplementedDataspaceServiceServer()
}

// UnimplementedDataspaceServiceServer must be embedded to have forward compatible implementations.
type UnimplementedDataspaceServiceServer struct {
}

func (UnimplementedDataspaceServiceServer) AddDataspace(context.Context, *AddDataspaceRequest) (*AddDataspaceReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddDataspace not implemented")
}
func (UnimplementedDataspaceServiceServer) DropDataspace(context.Context, *DropDataspaceRequest) (*DropDataspaceReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DropDataspace not implemented")
}
func (UnimplementedDataspaceServiceServer) ListDataspace(context.Context, *ListDataspaceRequest) (*ListDataspaceReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListDataspace not implemented")
}
func (UnimplementedDataspaceServiceServer) mustEmbedUnimplementedDataspaceServiceServer() {}

// UnsafeDataspaceServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DataspaceServiceServer will
// result in compilation errors.
type UnsafeDataspaceServiceServer interface {
	mustEmbedUnimplementedDataspaceServiceServer()
}

func RegisterDataspaceServiceServer(s grpc.ServiceRegistrar, srv DataspaceServiceServer) {
	s.RegisterService(&DataspaceService_ServiceDesc, srv)
}

func _DataspaceService_AddDataspace_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddDataspaceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataspaceServiceServer).AddDataspace(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/spqr.DataspaceService/AddDataspace",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataspaceServiceServer).AddDataspace(ctx, req.(*AddDataspaceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DataspaceService_DropDataspace_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DropDataspaceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataspaceServiceServer).DropDataspace(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/spqr.DataspaceService/DropDataspace",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataspaceServiceServer).DropDataspace(ctx, req.(*DropDataspaceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DataspaceService_ListDataspace_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListDataspaceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataspaceServiceServer).ListDataspace(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/spqr.DataspaceService/ListDataspace",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataspaceServiceServer).ListDataspace(ctx, req.(*ListDataspaceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// DataspaceService_ServiceDesc is the grpc.ServiceDesc for DataspaceService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var DataspaceService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "spqr.DataspaceService",
	HandlerType: (*DataspaceServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "AddDataspace",
			Handler:    _DataspaceService_AddDataspace_Handler,
		},
		{
			MethodName: "DropDataspace",
			Handler:    _DataspaceService_DropDataspace_Handler,
		},
		{
			MethodName: "ListDataspace",
			Handler:    _DataspaceService_ListDataspace_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "protos/dataspace.proto",
}
