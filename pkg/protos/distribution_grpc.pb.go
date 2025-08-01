// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v3.21.12
// source: protos/distribution.proto

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
	DistributionService_CreateDistribution_FullMethodName       = "/spqr.DistributionService/CreateDistribution"
	DistributionService_DropDistribution_FullMethodName         = "/spqr.DistributionService/DropDistribution"
	DistributionService_ListDistributions_FullMethodName        = "/spqr.DistributionService/ListDistributions"
	DistributionService_AlterDistributionAttach_FullMethodName  = "/spqr.DistributionService/AlterDistributionAttach"
	DistributionService_AlterDistributionDetach_FullMethodName  = "/spqr.DistributionService/AlterDistributionDetach"
	DistributionService_AlterDistributedRelation_FullMethodName = "/spqr.DistributionService/AlterDistributedRelation"
	DistributionService_GetDistribution_FullMethodName          = "/spqr.DistributionService/GetDistribution"
	DistributionService_GetRelationDistribution_FullMethodName  = "/spqr.DistributionService/GetRelationDistribution"
	DistributionService_NextRange_FullMethodName                = "/spqr.DistributionService/NextRange"
	DistributionService_CurrVal_FullMethodName                  = "/spqr.DistributionService/CurrVal"
	DistributionService_ListSequences_FullMethodName            = "/spqr.DistributionService/ListSequences"
	DistributionService_ListRelationSequences_FullMethodName    = "/spqr.DistributionService/ListRelationSequences"
	DistributionService_DropSequence_FullMethodName             = "/spqr.DistributionService/DropSequence"
)

// DistributionServiceClient is the client API for DistributionService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DistributionServiceClient interface {
	CreateDistribution(ctx context.Context, in *CreateDistributionRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	DropDistribution(ctx context.Context, in *DropDistributionRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	ListDistributions(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListDistributionsReply, error)
	AlterDistributionAttach(ctx context.Context, in *AlterDistributionAttachRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	AlterDistributionDetach(ctx context.Context, in *AlterDistributionDetachRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	AlterDistributedRelation(ctx context.Context, in *AlterDistributedRelationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	GetDistribution(ctx context.Context, in *GetDistributionRequest, opts ...grpc.CallOption) (*GetDistributionReply, error)
	GetRelationDistribution(ctx context.Context, in *GetRelationDistributionRequest, opts ...grpc.CallOption) (*GetRelationDistributionReply, error)
	NextRange(ctx context.Context, in *NextRangeRequest, opts ...grpc.CallOption) (*NextRangeReply, error)
	CurrVal(ctx context.Context, in *CurrValRequest, opts ...grpc.CallOption) (*CurrValReply, error)
	ListSequences(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListSequencesReply, error)
	ListRelationSequences(ctx context.Context, in *ListRelationSequencesRequest, opts ...grpc.CallOption) (*ListRelationSequencesReply, error)
	DropSequence(ctx context.Context, in *DropSequenceRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type distributionServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewDistributionServiceClient(cc grpc.ClientConnInterface) DistributionServiceClient {
	return &distributionServiceClient{cc}
}

func (c *distributionServiceClient) CreateDistribution(ctx context.Context, in *CreateDistributionRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, DistributionService_CreateDistribution_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *distributionServiceClient) DropDistribution(ctx context.Context, in *DropDistributionRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, DistributionService_DropDistribution_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *distributionServiceClient) ListDistributions(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListDistributionsReply, error) {
	out := new(ListDistributionsReply)
	err := c.cc.Invoke(ctx, DistributionService_ListDistributions_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *distributionServiceClient) AlterDistributionAttach(ctx context.Context, in *AlterDistributionAttachRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, DistributionService_AlterDistributionAttach_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *distributionServiceClient) AlterDistributionDetach(ctx context.Context, in *AlterDistributionDetachRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, DistributionService_AlterDistributionDetach_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *distributionServiceClient) AlterDistributedRelation(ctx context.Context, in *AlterDistributedRelationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, DistributionService_AlterDistributedRelation_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *distributionServiceClient) GetDistribution(ctx context.Context, in *GetDistributionRequest, opts ...grpc.CallOption) (*GetDistributionReply, error) {
	out := new(GetDistributionReply)
	err := c.cc.Invoke(ctx, DistributionService_GetDistribution_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *distributionServiceClient) GetRelationDistribution(ctx context.Context, in *GetRelationDistributionRequest, opts ...grpc.CallOption) (*GetRelationDistributionReply, error) {
	out := new(GetRelationDistributionReply)
	err := c.cc.Invoke(ctx, DistributionService_GetRelationDistribution_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *distributionServiceClient) NextRange(ctx context.Context, in *NextRangeRequest, opts ...grpc.CallOption) (*NextRangeReply, error) {
	out := new(NextRangeReply)
	err := c.cc.Invoke(ctx, DistributionService_NextRange_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *distributionServiceClient) CurrVal(ctx context.Context, in *CurrValRequest, opts ...grpc.CallOption) (*CurrValReply, error) {
	out := new(CurrValReply)
	err := c.cc.Invoke(ctx, DistributionService_CurrVal_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *distributionServiceClient) ListSequences(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListSequencesReply, error) {
	out := new(ListSequencesReply)
	err := c.cc.Invoke(ctx, DistributionService_ListSequences_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *distributionServiceClient) ListRelationSequences(ctx context.Context, in *ListRelationSequencesRequest, opts ...grpc.CallOption) (*ListRelationSequencesReply, error) {
	out := new(ListRelationSequencesReply)
	err := c.cc.Invoke(ctx, DistributionService_ListRelationSequences_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *distributionServiceClient) DropSequence(ctx context.Context, in *DropSequenceRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, DistributionService_DropSequence_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DistributionServiceServer is the server API for DistributionService service.
// All implementations must embed UnimplementedDistributionServiceServer
// for forward compatibility
type DistributionServiceServer interface {
	CreateDistribution(context.Context, *CreateDistributionRequest) (*emptypb.Empty, error)
	DropDistribution(context.Context, *DropDistributionRequest) (*emptypb.Empty, error)
	ListDistributions(context.Context, *emptypb.Empty) (*ListDistributionsReply, error)
	AlterDistributionAttach(context.Context, *AlterDistributionAttachRequest) (*emptypb.Empty, error)
	AlterDistributionDetach(context.Context, *AlterDistributionDetachRequest) (*emptypb.Empty, error)
	AlterDistributedRelation(context.Context, *AlterDistributedRelationRequest) (*emptypb.Empty, error)
	GetDistribution(context.Context, *GetDistributionRequest) (*GetDistributionReply, error)
	GetRelationDistribution(context.Context, *GetRelationDistributionRequest) (*GetRelationDistributionReply, error)
	NextRange(context.Context, *NextRangeRequest) (*NextRangeReply, error)
	CurrVal(context.Context, *CurrValRequest) (*CurrValReply, error)
	ListSequences(context.Context, *emptypb.Empty) (*ListSequencesReply, error)
	ListRelationSequences(context.Context, *ListRelationSequencesRequest) (*ListRelationSequencesReply, error)
	DropSequence(context.Context, *DropSequenceRequest) (*emptypb.Empty, error)
	mustEmbedUnimplementedDistributionServiceServer()
}

// UnimplementedDistributionServiceServer must be embedded to have forward compatible implementations.
type UnimplementedDistributionServiceServer struct {
}

func (UnimplementedDistributionServiceServer) CreateDistribution(context.Context, *CreateDistributionRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateDistribution not implemented")
}
func (UnimplementedDistributionServiceServer) DropDistribution(context.Context, *DropDistributionRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DropDistribution not implemented")
}
func (UnimplementedDistributionServiceServer) ListDistributions(context.Context, *emptypb.Empty) (*ListDistributionsReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListDistributions not implemented")
}
func (UnimplementedDistributionServiceServer) AlterDistributionAttach(context.Context, *AlterDistributionAttachRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AlterDistributionAttach not implemented")
}
func (UnimplementedDistributionServiceServer) AlterDistributionDetach(context.Context, *AlterDistributionDetachRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AlterDistributionDetach not implemented")
}
func (UnimplementedDistributionServiceServer) AlterDistributedRelation(context.Context, *AlterDistributedRelationRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AlterDistributedRelation not implemented")
}
func (UnimplementedDistributionServiceServer) GetDistribution(context.Context, *GetDistributionRequest) (*GetDistributionReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetDistribution not implemented")
}
func (UnimplementedDistributionServiceServer) GetRelationDistribution(context.Context, *GetRelationDistributionRequest) (*GetRelationDistributionReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetRelationDistribution not implemented")
}
func (UnimplementedDistributionServiceServer) NextRange(context.Context, *NextRangeRequest) (*NextRangeReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method NextRange not implemented")
}
func (UnimplementedDistributionServiceServer) CurrVal(context.Context, *CurrValRequest) (*CurrValReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CurrVal not implemented")
}
func (UnimplementedDistributionServiceServer) ListSequences(context.Context, *emptypb.Empty) (*ListSequencesReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListSequences not implemented")
}
func (UnimplementedDistributionServiceServer) ListRelationSequences(context.Context, *ListRelationSequencesRequest) (*ListRelationSequencesReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListRelationSequences not implemented")
}
func (UnimplementedDistributionServiceServer) DropSequence(context.Context, *DropSequenceRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DropSequence not implemented")
}
func (UnimplementedDistributionServiceServer) mustEmbedUnimplementedDistributionServiceServer() {}

// UnsafeDistributionServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DistributionServiceServer will
// result in compilation errors.
type UnsafeDistributionServiceServer interface {
	mustEmbedUnimplementedDistributionServiceServer()
}

func RegisterDistributionServiceServer(s grpc.ServiceRegistrar, srv DistributionServiceServer) {
	s.RegisterService(&DistributionService_ServiceDesc, srv)
}

func _DistributionService_CreateDistribution_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateDistributionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DistributionServiceServer).CreateDistribution(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DistributionService_CreateDistribution_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DistributionServiceServer).CreateDistribution(ctx, req.(*CreateDistributionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DistributionService_DropDistribution_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DropDistributionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DistributionServiceServer).DropDistribution(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DistributionService_DropDistribution_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DistributionServiceServer).DropDistribution(ctx, req.(*DropDistributionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DistributionService_ListDistributions_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DistributionServiceServer).ListDistributions(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DistributionService_ListDistributions_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DistributionServiceServer).ListDistributions(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _DistributionService_AlterDistributionAttach_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AlterDistributionAttachRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DistributionServiceServer).AlterDistributionAttach(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DistributionService_AlterDistributionAttach_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DistributionServiceServer).AlterDistributionAttach(ctx, req.(*AlterDistributionAttachRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DistributionService_AlterDistributionDetach_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AlterDistributionDetachRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DistributionServiceServer).AlterDistributionDetach(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DistributionService_AlterDistributionDetach_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DistributionServiceServer).AlterDistributionDetach(ctx, req.(*AlterDistributionDetachRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DistributionService_AlterDistributedRelation_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AlterDistributedRelationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DistributionServiceServer).AlterDistributedRelation(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DistributionService_AlterDistributedRelation_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DistributionServiceServer).AlterDistributedRelation(ctx, req.(*AlterDistributedRelationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DistributionService_GetDistribution_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetDistributionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DistributionServiceServer).GetDistribution(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DistributionService_GetDistribution_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DistributionServiceServer).GetDistribution(ctx, req.(*GetDistributionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DistributionService_GetRelationDistribution_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRelationDistributionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DistributionServiceServer).GetRelationDistribution(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DistributionService_GetRelationDistribution_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DistributionServiceServer).GetRelationDistribution(ctx, req.(*GetRelationDistributionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DistributionService_NextRange_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NextRangeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DistributionServiceServer).NextRange(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DistributionService_NextRange_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DistributionServiceServer).NextRange(ctx, req.(*NextRangeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DistributionService_CurrVal_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CurrValRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DistributionServiceServer).CurrVal(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DistributionService_CurrVal_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DistributionServiceServer).CurrVal(ctx, req.(*CurrValRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DistributionService_ListSequences_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DistributionServiceServer).ListSequences(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DistributionService_ListSequences_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DistributionServiceServer).ListSequences(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _DistributionService_ListRelationSequences_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListRelationSequencesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DistributionServiceServer).ListRelationSequences(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DistributionService_ListRelationSequences_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DistributionServiceServer).ListRelationSequences(ctx, req.(*ListRelationSequencesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DistributionService_DropSequence_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DropSequenceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DistributionServiceServer).DropSequence(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DistributionService_DropSequence_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DistributionServiceServer).DropSequence(ctx, req.(*DropSequenceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// DistributionService_ServiceDesc is the grpc.ServiceDesc for DistributionService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var DistributionService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "spqr.DistributionService",
	HandlerType: (*DistributionServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateDistribution",
			Handler:    _DistributionService_CreateDistribution_Handler,
		},
		{
			MethodName: "DropDistribution",
			Handler:    _DistributionService_DropDistribution_Handler,
		},
		{
			MethodName: "ListDistributions",
			Handler:    _DistributionService_ListDistributions_Handler,
		},
		{
			MethodName: "AlterDistributionAttach",
			Handler:    _DistributionService_AlterDistributionAttach_Handler,
		},
		{
			MethodName: "AlterDistributionDetach",
			Handler:    _DistributionService_AlterDistributionDetach_Handler,
		},
		{
			MethodName: "AlterDistributedRelation",
			Handler:    _DistributionService_AlterDistributedRelation_Handler,
		},
		{
			MethodName: "GetDistribution",
			Handler:    _DistributionService_GetDistribution_Handler,
		},
		{
			MethodName: "GetRelationDistribution",
			Handler:    _DistributionService_GetRelationDistribution_Handler,
		},
		{
			MethodName: "NextRange",
			Handler:    _DistributionService_NextRange_Handler,
		},
		{
			MethodName: "CurrVal",
			Handler:    _DistributionService_CurrVal_Handler,
		},
		{
			MethodName: "ListSequences",
			Handler:    _DistributionService_ListSequences_Handler,
		},
		{
			MethodName: "ListRelationSequences",
			Handler:    _DistributionService_ListRelationSequences_Handler,
		},
		{
			MethodName: "DropSequence",
			Handler:    _DistributionService_DropSequence_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "protos/distribution.proto",
}
