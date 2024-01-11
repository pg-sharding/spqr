// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        v3.6.1
// source: protos/backend_connections.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ListBackendConnectionsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *ListBackendConnectionsRequest) Reset() {
	*x = ListBackendConnectionsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_backend_connections_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListBackendConnectionsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListBackendConnectionsRequest) ProtoMessage() {}

func (x *ListBackendConnectionsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_protos_backend_connections_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListBackendConnectionsRequest.ProtoReflect.Descriptor instead.
func (*ListBackendConnectionsRequest) Descriptor() ([]byte, []int) {
	return file_protos_backend_connections_proto_rawDescGZIP(), []int{0}
}

type ListBackendConntionsReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Conns []*BackendConnectionsInfo `protobuf:"bytes,1,rep,name=conns,proto3" json:"conns,omitempty"`
}

func (x *ListBackendConntionsReply) Reset() {
	*x = ListBackendConntionsReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_backend_connections_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListBackendConntionsReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListBackendConntionsReply) ProtoMessage() {}

func (x *ListBackendConntionsReply) ProtoReflect() protoreflect.Message {
	mi := &file_protos_backend_connections_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListBackendConntionsReply.ProtoReflect.Descriptor instead.
func (*ListBackendConntionsReply) Descriptor() ([]byte, []int) {
	return file_protos_backend_connections_proto_rawDescGZIP(), []int{1}
}

func (x *ListBackendConntionsReply) GetConns() []*BackendConnectionsInfo {
	if x != nil {
		return x.Conns
	}
	return nil
}

type BackendConnectionsInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	BackendConnectionId uint64 `protobuf:"varint,1,opt,name=backend_connection_id,json=backendConnectionId,proto3" json:"backend_connection_id,omitempty"`
	ShardKeyName        string `protobuf:"bytes,2,opt,name=shard_key_name,json=shardKeyName,proto3" json:"shard_key_name,omitempty"`
	Hostname            string `protobuf:"bytes,3,opt,name=hostname,proto3" json:"hostname,omitempty"`
	User                string `protobuf:"bytes,4,opt,name=user,proto3" json:"user,omitempty"`
	Dbname              string `protobuf:"bytes,5,opt,name=dbname,proto3" json:"dbname,omitempty"`
	Sync                int64  `protobuf:"varint,6,opt,name=sync,proto3" json:"sync,omitempty"`
	TxServed            int64  `protobuf:"varint,7,opt,name=tx_served,json=txServed,proto3" json:"tx_served,omitempty"`
	TxStatus            int64  `protobuf:"varint,8,opt,name=tx_status,json=txStatus,proto3" json:"tx_status,omitempty"`
}

func (x *BackendConnectionsInfo) Reset() {
	*x = BackendConnectionsInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_backend_connections_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BackendConnectionsInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BackendConnectionsInfo) ProtoMessage() {}

func (x *BackendConnectionsInfo) ProtoReflect() protoreflect.Message {
	mi := &file_protos_backend_connections_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BackendConnectionsInfo.ProtoReflect.Descriptor instead.
func (*BackendConnectionsInfo) Descriptor() ([]byte, []int) {
	return file_protos_backend_connections_proto_rawDescGZIP(), []int{2}
}

func (x *BackendConnectionsInfo) GetBackendConnectionId() uint64 {
	if x != nil {
		return x.BackendConnectionId
	}
	return 0
}

func (x *BackendConnectionsInfo) GetShardKeyName() string {
	if x != nil {
		return x.ShardKeyName
	}
	return ""
}

func (x *BackendConnectionsInfo) GetHostname() string {
	if x != nil {
		return x.Hostname
	}
	return ""
}

func (x *BackendConnectionsInfo) GetUser() string {
	if x != nil {
		return x.User
	}
	return ""
}

func (x *BackendConnectionsInfo) GetDbname() string {
	if x != nil {
		return x.Dbname
	}
	return ""
}

func (x *BackendConnectionsInfo) GetSync() int64 {
	if x != nil {
		return x.Sync
	}
	return 0
}

func (x *BackendConnectionsInfo) GetTxServed() int64 {
	if x != nil {
		return x.TxServed
	}
	return 0
}

func (x *BackendConnectionsInfo) GetTxStatus() int64 {
	if x != nil {
		return x.TxStatus
	}
	return 0
}

var File_protos_backend_connections_proto protoreflect.FileDescriptor

var file_protos_backend_connections_proto_rawDesc = []byte{
	0x0a, 0x20, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f, 0x62, 0x61, 0x63, 0x6b, 0x65, 0x6e, 0x64,
	0x5f, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x04, 0x73, 0x70, 0x71, 0x72, 0x22, 0x1f, 0x0a, 0x1d, 0x4c, 0x69, 0x73, 0x74,
	0x42, 0x61, 0x63, 0x6b, 0x65, 0x6e, 0x64, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f,
	0x6e, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x4f, 0x0a, 0x19, 0x4c, 0x69, 0x73,
	0x74, 0x42, 0x61, 0x63, 0x6b, 0x65, 0x6e, 0x64, 0x43, 0x6f, 0x6e, 0x6e, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x32, 0x0a, 0x05, 0x63, 0x6f, 0x6e, 0x6e, 0x73, 0x18,
	0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x73, 0x70, 0x71, 0x72, 0x2e, 0x42, 0x61, 0x63,
	0x6b, 0x65, 0x6e, 0x64, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x49,
	0x6e, 0x66, 0x6f, 0x52, 0x05, 0x63, 0x6f, 0x6e, 0x6e, 0x73, 0x22, 0x88, 0x02, 0x0a, 0x16, 0x42,
	0x61, 0x63, 0x6b, 0x65, 0x6e, 0x64, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x32, 0x0a, 0x15, 0x62, 0x61, 0x63, 0x6b, 0x65, 0x6e, 0x64,
	0x5f, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x13, 0x62, 0x61, 0x63, 0x6b, 0x65, 0x6e, 0x64, 0x43, 0x6f, 0x6e,
	0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x24, 0x0a, 0x0e, 0x73, 0x68, 0x61,
	0x72, 0x64, 0x5f, 0x6b, 0x65, 0x79, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0c, 0x73, 0x68, 0x61, 0x72, 0x64, 0x4b, 0x65, 0x79, 0x4e, 0x61, 0x6d, 0x65, 0x12,
	0x1a, 0x0a, 0x08, 0x68, 0x6f, 0x73, 0x74, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x08, 0x68, 0x6f, 0x73, 0x74, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x75,
	0x73, 0x65, 0x72, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x75, 0x73, 0x65, 0x72, 0x12,
	0x16, 0x0a, 0x06, 0x64, 0x62, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x06, 0x64, 0x62, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x73, 0x79, 0x6e, 0x63, 0x18,
	0x06, 0x20, 0x01, 0x28, 0x03, 0x52, 0x04, 0x73, 0x79, 0x6e, 0x63, 0x12, 0x1b, 0x0a, 0x09, 0x74,
	0x78, 0x5f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x64, 0x18, 0x07, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08,
	0x74, 0x78, 0x53, 0x65, 0x72, 0x76, 0x65, 0x64, 0x12, 0x1b, 0x0a, 0x09, 0x74, 0x78, 0x5f, 0x73,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x08, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x74, 0x78, 0x53,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x32, 0x7d, 0x0a, 0x19, 0x42, 0x61, 0x63, 0x6b, 0x65, 0x6e, 0x64,
	0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x53, 0x65, 0x72, 0x76, 0x69,
	0x63, 0x65, 0x12, 0x60, 0x0a, 0x16, 0x4c, 0x69, 0x73, 0x74, 0x42, 0x61, 0x63, 0x6b, 0x65, 0x6e,
	0x64, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x23, 0x2e, 0x73,
	0x70, 0x71, 0x72, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x42, 0x61, 0x63, 0x6b, 0x65, 0x6e, 0x64, 0x43,
	0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x1f, 0x2e, 0x73, 0x70, 0x71, 0x72, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x42, 0x61, 0x63,
	0x6b, 0x65, 0x6e, 0x64, 0x43, 0x6f, 0x6e, 0x6e, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x52, 0x65, 0x70,
	0x6c, 0x79, 0x22, 0x00, 0x42, 0x0c, 0x5a, 0x0a, 0x73, 0x70, 0x71, 0x72, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_protos_backend_connections_proto_rawDescOnce sync.Once
	file_protos_backend_connections_proto_rawDescData = file_protos_backend_connections_proto_rawDesc
)

func file_protos_backend_connections_proto_rawDescGZIP() []byte {
	file_protos_backend_connections_proto_rawDescOnce.Do(func() {
		file_protos_backend_connections_proto_rawDescData = protoimpl.X.CompressGZIP(file_protos_backend_connections_proto_rawDescData)
	})
	return file_protos_backend_connections_proto_rawDescData
}

var file_protos_backend_connections_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_protos_backend_connections_proto_goTypes = []interface{}{
	(*ListBackendConnectionsRequest)(nil), // 0: spqr.ListBackendConnectionsRequest
	(*ListBackendConntionsReply)(nil),     // 1: spqr.ListBackendConntionsReply
	(*BackendConnectionsInfo)(nil),        // 2: spqr.BackendConnectionsInfo
}
var file_protos_backend_connections_proto_depIdxs = []int32{
	2, // 0: spqr.ListBackendConntionsReply.conns:type_name -> spqr.BackendConnectionsInfo
	0, // 1: spqr.BackendConnectionsService.ListBackendConnections:input_type -> spqr.ListBackendConnectionsRequest
	1, // 2: spqr.BackendConnectionsService.ListBackendConnections:output_type -> spqr.ListBackendConntionsReply
	2, // [2:3] is the sub-list for method output_type
	1, // [1:2] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_protos_backend_connections_proto_init() }
func file_protos_backend_connections_proto_init() {
	if File_protos_backend_connections_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_protos_backend_connections_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListBackendConnectionsRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_backend_connections_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListBackendConntionsReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_backend_connections_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BackendConnectionsInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_protos_backend_connections_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_protos_backend_connections_proto_goTypes,
		DependencyIndexes: file_protos_backend_connections_proto_depIdxs,
		MessageInfos:      file_protos_backend_connections_proto_msgTypes,
	}.Build()
	File_protos_backend_connections_proto = out.File
	file_protos_backend_connections_proto_rawDesc = nil
	file_protos_backend_connections_proto_goTypes = nil
	file_protos_backend_connections_proto_depIdxs = nil
}
