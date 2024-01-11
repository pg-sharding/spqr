// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v3.12.4
// source: protos/clients.proto

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

const (
	ClientInfoService_ListClients_FullMethodName = "/spqr.ClientInfoService/ListClients"
)

// ClientInfoServiceClient is the client API for ClientInfoService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ClientInfoServiceClient interface {
	ListClients(ctx context.Context, in *ListClientsRequest, opts ...grpc.CallOption) (*ListClientsReply, error)
}

type clientInfoServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewClientInfoServiceClient(cc grpc.ClientConnInterface) ClientInfoServiceClient {
	return &clientInfoServiceClient{cc}
}

func (c *clientInfoServiceClient) ListClients(ctx context.Context, in *ListClientsRequest, opts ...grpc.CallOption) (*ListClientsReply, error) {
	out := new(ListClientsReply)
	err := c.cc.Invoke(ctx, ClientInfoService_ListClients_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ClientInfoServiceServer is the server API for ClientInfoService service.
// All implementations must embed UnimplementedClientInfoServiceServer
// for forward compatibility
type ClientInfoServiceServer interface {
	ListClients(context.Context, *ListClientsRequest) (*ListClientsReply, error)
	mustEmbedUnimplementedClientInfoServiceServer()
}

// UnimplementedClientInfoServiceServer must be embedded to have forward compatible implementations.
type UnimplementedClientInfoServiceServer struct {
}

func (UnimplementedClientInfoServiceServer) ListClients(context.Context, *ListClientsRequest) (*ListClientsReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListClients not implemented")
}
func (UnimplementedClientInfoServiceServer) mustEmbedUnimplementedClientInfoServiceServer() {}

// UnsafeClientInfoServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ClientInfoServiceServer will
// result in compilation errors.
type UnsafeClientInfoServiceServer interface {
	mustEmbedUnimplementedClientInfoServiceServer()
}

func RegisterClientInfoServiceServer(s grpc.ServiceRegistrar, srv ClientInfoServiceServer) {
	s.RegisterService(&ClientInfoService_ServiceDesc, srv)
}

func _ClientInfoService_ListClients_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListClientsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClientInfoServiceServer).ListClients(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ClientInfoService_ListClients_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClientInfoServiceServer).ListClients(ctx, req.(*ListClientsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ClientInfoService_ServiceDesc is the grpc.ServiceDesc for ClientInfoService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ClientInfoService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "spqr.ClientInfoService",
	HandlerType: (*ClientInfoServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ListClients",
			Handler:    _ClientInfoService_ListClients_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "protos/clients.proto",
}
