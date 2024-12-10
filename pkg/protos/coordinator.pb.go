// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.1
// 	protoc        v5.28.3
// source: protos/coordinator.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type GetRouterStatusReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Status RouterStatus `protobuf:"varint,1,opt,name=status,proto3,enum=spqr.RouterStatus" json:"status,omitempty"`
}

func (x *GetRouterStatusReply) Reset() {
	*x = GetRouterStatusReply{}
	mi := &file_protos_coordinator_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetRouterStatusReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetRouterStatusReply) ProtoMessage() {}

func (x *GetRouterStatusReply) ProtoReflect() protoreflect.Message {
	mi := &file_protos_coordinator_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetRouterStatusReply.ProtoReflect.Descriptor instead.
func (*GetRouterStatusReply) Descriptor() ([]byte, []int) {
	return file_protos_coordinator_proto_rawDescGZIP(), []int{0}
}

func (x *GetRouterStatusReply) GetStatus() RouterStatus {
	if x != nil {
		return x.Status
	}
	return RouterStatus_CLOSED
}

type UpdateCoordinatorRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Address string `protobuf:"bytes,1,opt,name=address,proto3" json:"address,omitempty"`
}

func (x *UpdateCoordinatorRequest) Reset() {
	*x = UpdateCoordinatorRequest{}
	mi := &file_protos_coordinator_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdateCoordinatorRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateCoordinatorRequest) ProtoMessage() {}

func (x *UpdateCoordinatorRequest) ProtoReflect() protoreflect.Message {
	mi := &file_protos_coordinator_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateCoordinatorRequest.ProtoReflect.Descriptor instead.
func (*UpdateCoordinatorRequest) Descriptor() ([]byte, []int) {
	return file_protos_coordinator_proto_rawDescGZIP(), []int{1}
}

func (x *UpdateCoordinatorRequest) GetAddress() string {
	if x != nil {
		return x.Address
	}
	return ""
}

type GetCoordinatorResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Address string `protobuf:"bytes,1,opt,name=address,proto3" json:"address,omitempty"`
}

func (x *GetCoordinatorResponse) Reset() {
	*x = GetCoordinatorResponse{}
	mi := &file_protos_coordinator_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetCoordinatorResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetCoordinatorResponse) ProtoMessage() {}

func (x *GetCoordinatorResponse) ProtoReflect() protoreflect.Message {
	mi := &file_protos_coordinator_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetCoordinatorResponse.ProtoReflect.Descriptor instead.
func (*GetCoordinatorResponse) Descriptor() ([]byte, []int) {
	return file_protos_coordinator_proto_rawDescGZIP(), []int{2}
}

func (x *GetCoordinatorResponse) GetAddress() string {
	if x != nil {
		return x.Address
	}
	return ""
}

var File_protos_coordinator_proto protoreflect.FileDescriptor

var file_protos_coordinator_proto_rawDesc = []byte{
	0x0a, 0x18, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f, 0x63, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e,
	0x61, 0x74, 0x6f, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04, 0x73, 0x70, 0x71, 0x72,
	0x1a, 0x1b, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2f, 0x65, 0x6d, 0x70, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x13, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f, 0x72, 0x6f, 0x75, 0x74, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0x42, 0x0a, 0x14, 0x47, 0x65, 0x74, 0x52, 0x6f, 0x75, 0x74, 0x65, 0x72, 0x53,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x2a, 0x0a, 0x06, 0x73, 0x74,
	0x61, 0x74, 0x75, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x12, 0x2e, 0x73, 0x70, 0x71,
	0x72, 0x2e, 0x52, 0x6f, 0x75, 0x74, 0x65, 0x72, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06,
	0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x22, 0x34, 0x0a, 0x18, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65,
	0x43, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x74, 0x6f, 0x72, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x18, 0x0a, 0x07, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x22, 0x32, 0x0a, 0x16,
	0x47, 0x65, 0x74, 0x43, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x74, 0x6f, 0x72, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73,
	0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73,
	0x32, 0xf4, 0x02, 0x0a, 0x0f, 0x54, 0x6f, 0x70, 0x6f, 0x6c, 0x6f, 0x67, 0x79, 0x53, 0x65, 0x72,
	0x76, 0x69, 0x63, 0x65, 0x12, 0x3e, 0x0a, 0x0a, 0x4f, 0x70, 0x65, 0x6e, 0x52, 0x6f, 0x75, 0x74,
	0x65, 0x72, 0x12, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x1a, 0x16, 0x2e, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70,
	0x74, 0x79, 0x22, 0x00, 0x12, 0x47, 0x0a, 0x0f, 0x47, 0x65, 0x74, 0x52, 0x6f, 0x75, 0x74, 0x65,
	0x72, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x1a,
	0x1a, 0x2e, 0x73, 0x70, 0x71, 0x72, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x6f, 0x75, 0x74, 0x65, 0x72,
	0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x00, 0x12, 0x3f, 0x0a,
	0x0b, 0x43, 0x6c, 0x6f, 0x73, 0x65, 0x52, 0x6f, 0x75, 0x74, 0x65, 0x72, 0x12, 0x16, 0x2e, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45,
	0x6d, 0x70, 0x74, 0x79, 0x1a, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x22, 0x00, 0x12, 0x4d,
	0x0a, 0x11, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x43, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61,
	0x74, 0x6f, 0x72, 0x12, 0x1e, 0x2e, 0x73, 0x70, 0x71, 0x72, 0x2e, 0x55, 0x70, 0x64, 0x61, 0x74,
	0x65, 0x43, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x74, 0x6f, 0x72, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x22, 0x00, 0x12, 0x48, 0x0a,
	0x0e, 0x47, 0x65, 0x74, 0x43, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x74, 0x6f, 0x72, 0x12,
	0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x1a, 0x1c, 0x2e, 0x73, 0x70, 0x71, 0x72, 0x2e, 0x47,
	0x65, 0x74, 0x43, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x74, 0x6f, 0x72, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x0c, 0x5a, 0x0a, 0x73, 0x70, 0x71, 0x72, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_protos_coordinator_proto_rawDescOnce sync.Once
	file_protos_coordinator_proto_rawDescData = file_protos_coordinator_proto_rawDesc
)

func file_protos_coordinator_proto_rawDescGZIP() []byte {
	file_protos_coordinator_proto_rawDescOnce.Do(func() {
		file_protos_coordinator_proto_rawDescData = protoimpl.X.CompressGZIP(file_protos_coordinator_proto_rawDescData)
	})
	return file_protos_coordinator_proto_rawDescData
}

var file_protos_coordinator_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_protos_coordinator_proto_goTypes = []any{
	(*GetRouterStatusReply)(nil),     // 0: spqr.GetRouterStatusReply
	(*UpdateCoordinatorRequest)(nil), // 1: spqr.UpdateCoordinatorRequest
	(*GetCoordinatorResponse)(nil),   // 2: spqr.GetCoordinatorResponse
	(RouterStatus)(0),                // 3: spqr.RouterStatus
	(*emptypb.Empty)(nil),            // 4: google.protobuf.Empty
}
var file_protos_coordinator_proto_depIdxs = []int32{
	3, // 0: spqr.GetRouterStatusReply.status:type_name -> spqr.RouterStatus
	4, // 1: spqr.TopologyService.OpenRouter:input_type -> google.protobuf.Empty
	4, // 2: spqr.TopologyService.GetRouterStatus:input_type -> google.protobuf.Empty
	4, // 3: spqr.TopologyService.CloseRouter:input_type -> google.protobuf.Empty
	1, // 4: spqr.TopologyService.UpdateCoordinator:input_type -> spqr.UpdateCoordinatorRequest
	4, // 5: spqr.TopologyService.GetCoordinator:input_type -> google.protobuf.Empty
	4, // 6: spqr.TopologyService.OpenRouter:output_type -> google.protobuf.Empty
	0, // 7: spqr.TopologyService.GetRouterStatus:output_type -> spqr.GetRouterStatusReply
	4, // 8: spqr.TopologyService.CloseRouter:output_type -> google.protobuf.Empty
	4, // 9: spqr.TopologyService.UpdateCoordinator:output_type -> google.protobuf.Empty
	2, // 10: spqr.TopologyService.GetCoordinator:output_type -> spqr.GetCoordinatorResponse
	6, // [6:11] is the sub-list for method output_type
	1, // [1:6] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_protos_coordinator_proto_init() }
func file_protos_coordinator_proto_init() {
	if File_protos_coordinator_proto != nil {
		return
	}
	file_protos_router_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_protos_coordinator_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_protos_coordinator_proto_goTypes,
		DependencyIndexes: file_protos_coordinator_proto_depIdxs,
		MessageInfos:      file_protos_coordinator_proto_msgTypes,
	}.Build()
	File_protos_coordinator_proto = out.File
	file_protos_coordinator_proto_rawDesc = nil
	file_protos_coordinator_proto_goTypes = nil
	file_protos_coordinator_proto_depIdxs = nil
}
