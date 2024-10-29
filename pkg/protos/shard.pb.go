// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        v3.21.12
// source: protos/shard.proto

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

type Shard struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id    string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Hosts []string `protobuf:"bytes,2,rep,name=hosts,proto3" json:"hosts,omitempty"`
}

func (x *Shard) Reset() {
	*x = Shard{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_shard_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Shard) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Shard) ProtoMessage() {}

func (x *Shard) ProtoReflect() protoreflect.Message {
	mi := &file_protos_shard_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Shard.ProtoReflect.Descriptor instead.
func (*Shard) Descriptor() ([]byte, []int) {
	return file_protos_shard_proto_rawDescGZIP(), []int{0}
}

func (x *Shard) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Shard) GetHosts() []string {
	if x != nil {
		return x.Hosts
	}
	return nil
}

type ShardInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id    string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Hosts []string `protobuf:"bytes,2,rep,name=hosts,proto3" json:"hosts,omitempty"`
}

func (x *ShardInfo) Reset() {
	*x = ShardInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_shard_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ShardInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ShardInfo) ProtoMessage() {}

func (x *ShardInfo) ProtoReflect() protoreflect.Message {
	mi := &file_protos_shard_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ShardInfo.ProtoReflect.Descriptor instead.
func (*ShardInfo) Descriptor() ([]byte, []int) {
	return file_protos_shard_proto_rawDescGZIP(), []int{1}
}

func (x *ShardInfo) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *ShardInfo) GetHosts() []string {
	if x != nil {
		return x.Hosts
	}
	return nil
}

type ShardReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Shard *Shard `protobuf:"bytes,1,opt,name=shard,proto3" json:"shard,omitempty"`
}

func (x *ShardReply) Reset() {
	*x = ShardReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_shard_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ShardReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ShardReply) ProtoMessage() {}

func (x *ShardReply) ProtoReflect() protoreflect.Message {
	mi := &file_protos_shard_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ShardReply.ProtoReflect.Descriptor instead.
func (*ShardReply) Descriptor() ([]byte, []int) {
	return file_protos_shard_proto_rawDescGZIP(), []int{2}
}

func (x *ShardReply) GetShard() *Shard {
	if x != nil {
		return x.Shard
	}
	return nil
}

type ShardRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
}

func (x *ShardRequest) Reset() {
	*x = ShardRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_shard_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ShardRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ShardRequest) ProtoMessage() {}

func (x *ShardRequest) ProtoReflect() protoreflect.Message {
	mi := &file_protos_shard_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ShardRequest.ProtoReflect.Descriptor instead.
func (*ShardRequest) Descriptor() ([]byte, []int) {
	return file_protos_shard_proto_rawDescGZIP(), []int{3}
}

func (x *ShardRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

type ListShardsReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Shards []*Shard `protobuf:"bytes,1,rep,name=shards,proto3" json:"shards,omitempty"`
}

func (x *ListShardsReply) Reset() {
	*x = ListShardsReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_shard_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListShardsReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListShardsReply) ProtoMessage() {}

func (x *ListShardsReply) ProtoReflect() protoreflect.Message {
	mi := &file_protos_shard_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListShardsReply.ProtoReflect.Descriptor instead.
func (*ListShardsReply) Descriptor() ([]byte, []int) {
	return file_protos_shard_proto_rawDescGZIP(), []int{4}
}

func (x *ListShardsReply) GetShards() []*Shard {
	if x != nil {
		return x.Shards
	}
	return nil
}

type AddShardRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Shard *Shard `protobuf:"bytes,1,opt,name=shard,proto3" json:"shard,omitempty"`
}

func (x *AddShardRequest) Reset() {
	*x = AddShardRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_shard_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AddShardRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AddShardRequest) ProtoMessage() {}

func (x *AddShardRequest) ProtoReflect() protoreflect.Message {
	mi := &file_protos_shard_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AddShardRequest.ProtoReflect.Descriptor instead.
func (*AddShardRequest) Descriptor() ([]byte, []int) {
	return file_protos_shard_proto_rawDescGZIP(), []int{5}
}

func (x *AddShardRequest) GetShard() *Shard {
	if x != nil {
		return x.Shard
	}
	return nil
}

type AddWorldShardRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Shard *Shard `protobuf:"bytes,1,opt,name=shard,proto3" json:"shard,omitempty"`
}

func (x *AddWorldShardRequest) Reset() {
	*x = AddWorldShardRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_shard_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AddWorldShardRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AddWorldShardRequest) ProtoMessage() {}

func (x *AddWorldShardRequest) ProtoReflect() protoreflect.Message {
	mi := &file_protos_shard_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AddWorldShardRequest.ProtoReflect.Descriptor instead.
func (*AddWorldShardRequest) Descriptor() ([]byte, []int) {
	return file_protos_shard_proto_rawDescGZIP(), []int{6}
}

func (x *AddWorldShardRequest) GetShard() *Shard {
	if x != nil {
		return x.Shard
	}
	return nil
}

var File_protos_shard_proto protoreflect.FileDescriptor

var file_protos_shard_proto_rawDesc = []byte{
	0x0a, 0x12, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f, 0x73, 0x68, 0x61, 0x72, 0x64, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04, 0x73, 0x70, 0x71, 0x72, 0x1a, 0x1b, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x65, 0x6d, 0x70, 0x74,
	0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x2d, 0x0a, 0x05, 0x53, 0x68, 0x61, 0x72, 0x64,
	0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64,
	0x12, 0x14, 0x0a, 0x05, 0x68, 0x6f, 0x73, 0x74, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52,
	0x05, 0x68, 0x6f, 0x73, 0x74, 0x73, 0x22, 0x31, 0x0a, 0x09, 0x53, 0x68, 0x61, 0x72, 0x64, 0x49,
	0x6e, 0x66, 0x6f, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x02, 0x69, 0x64, 0x12, 0x14, 0x0a, 0x05, 0x68, 0x6f, 0x73, 0x74, 0x73, 0x18, 0x02, 0x20, 0x03,
	0x28, 0x09, 0x52, 0x05, 0x68, 0x6f, 0x73, 0x74, 0x73, 0x22, 0x2f, 0x0a, 0x0a, 0x53, 0x68, 0x61,
	0x72, 0x64, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x21, 0x0a, 0x05, 0x73, 0x68, 0x61, 0x72, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x73, 0x70, 0x71, 0x72, 0x2e, 0x53, 0x68,
	0x61, 0x72, 0x64, 0x52, 0x05, 0x73, 0x68, 0x61, 0x72, 0x64, 0x22, 0x1e, 0x0a, 0x0c, 0x53, 0x68,
	0x61, 0x72, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x22, 0x36, 0x0a, 0x0f, 0x4c, 0x69,
	0x73, 0x74, 0x53, 0x68, 0x61, 0x72, 0x64, 0x73, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x23, 0x0a,
	0x06, 0x73, 0x68, 0x61, 0x72, 0x64, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0b, 0x2e,
	0x73, 0x70, 0x71, 0x72, 0x2e, 0x53, 0x68, 0x61, 0x72, 0x64, 0x52, 0x06, 0x73, 0x68, 0x61, 0x72,
	0x64, 0x73, 0x22, 0x34, 0x0a, 0x0f, 0x41, 0x64, 0x64, 0x53, 0x68, 0x61, 0x72, 0x64, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x05, 0x73, 0x68, 0x61, 0x72, 0x64, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x73, 0x70, 0x71, 0x72, 0x2e, 0x53, 0x68, 0x61, 0x72,
	0x64, 0x52, 0x05, 0x73, 0x68, 0x61, 0x72, 0x64, 0x22, 0x39, 0x0a, 0x14, 0x41, 0x64, 0x64, 0x57,
	0x6f, 0x72, 0x6c, 0x64, 0x53, 0x68, 0x61, 0x72, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x21, 0x0a, 0x05, 0x73, 0x68, 0x61, 0x72, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x0b, 0x2e, 0x73, 0x70, 0x71, 0x72, 0x2e, 0x53, 0x68, 0x61, 0x72, 0x64, 0x52, 0x05, 0x73, 0x68,
	0x61, 0x72, 0x64, 0x32, 0x89, 0x02, 0x0a, 0x0c, 0x53, 0x68, 0x61, 0x72, 0x64, 0x53, 0x65, 0x72,
	0x76, 0x69, 0x63, 0x65, 0x12, 0x3d, 0x0a, 0x0a, 0x4c, 0x69, 0x73, 0x74, 0x53, 0x68, 0x61, 0x72,
	0x64, 0x73, 0x12, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x1a, 0x15, 0x2e, 0x73, 0x70, 0x71,
	0x72, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x53, 0x68, 0x61, 0x72, 0x64, 0x73, 0x52, 0x65, 0x70, 0x6c,
	0x79, 0x22, 0x00, 0x12, 0x3f, 0x0a, 0x0c, 0x41, 0x64, 0x64, 0x44, 0x61, 0x74, 0x61, 0x53, 0x68,
	0x61, 0x72, 0x64, 0x12, 0x15, 0x2e, 0x73, 0x70, 0x71, 0x72, 0x2e, 0x41, 0x64, 0x64, 0x53, 0x68,
	0x61, 0x72, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x16, 0x2e, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70,
	0x74, 0x79, 0x22, 0x00, 0x12, 0x45, 0x0a, 0x0d, 0x41, 0x64, 0x64, 0x57, 0x6f, 0x72, 0x6c, 0x64,
	0x53, 0x68, 0x61, 0x72, 0x64, 0x12, 0x1a, 0x2e, 0x73, 0x70, 0x71, 0x72, 0x2e, 0x41, 0x64, 0x64,
	0x57, 0x6f, 0x72, 0x6c, 0x64, 0x53, 0x68, 0x61, 0x72, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x22, 0x00, 0x12, 0x32, 0x0a, 0x08, 0x47,
	0x65, 0x74, 0x53, 0x68, 0x61, 0x72, 0x64, 0x12, 0x12, 0x2e, 0x73, 0x70, 0x71, 0x72, 0x2e, 0x53,
	0x68, 0x61, 0x72, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x10, 0x2e, 0x73, 0x70,
	0x71, 0x72, 0x2e, 0x53, 0x68, 0x61, 0x72, 0x64, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x00, 0x42,
	0x0c, 0x5a, 0x0a, 0x73, 0x70, 0x71, 0x72, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_protos_shard_proto_rawDescOnce sync.Once
	file_protos_shard_proto_rawDescData = file_protos_shard_proto_rawDesc
)

func file_protos_shard_proto_rawDescGZIP() []byte {
	file_protos_shard_proto_rawDescOnce.Do(func() {
		file_protos_shard_proto_rawDescData = protoimpl.X.CompressGZIP(file_protos_shard_proto_rawDescData)
	})
	return file_protos_shard_proto_rawDescData
}

var file_protos_shard_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_protos_shard_proto_goTypes = []interface{}{
	(*Shard)(nil),                // 0: spqr.Shard
	(*ShardInfo)(nil),            // 1: spqr.ShardInfo
	(*ShardReply)(nil),           // 2: spqr.ShardReply
	(*ShardRequest)(nil),         // 3: spqr.ShardRequest
	(*ListShardsReply)(nil),      // 4: spqr.ListShardsReply
	(*AddShardRequest)(nil),      // 5: spqr.AddShardRequest
	(*AddWorldShardRequest)(nil), // 6: spqr.AddWorldShardRequest
	(*emptypb.Empty)(nil),        // 7: google.protobuf.Empty
}
var file_protos_shard_proto_depIdxs = []int32{
	0, // 0: spqr.ShardReply.shard:type_name -> spqr.Shard
	0, // 1: spqr.ListShardsReply.shards:type_name -> spqr.Shard
	0, // 2: spqr.AddShardRequest.shard:type_name -> spqr.Shard
	0, // 3: spqr.AddWorldShardRequest.shard:type_name -> spqr.Shard
	7, // 4: spqr.ShardService.ListShards:input_type -> google.protobuf.Empty
	5, // 5: spqr.ShardService.AddDataShard:input_type -> spqr.AddShardRequest
	6, // 6: spqr.ShardService.AddWorldShard:input_type -> spqr.AddWorldShardRequest
	3, // 7: spqr.ShardService.GetShard:input_type -> spqr.ShardRequest
	4, // 8: spqr.ShardService.ListShards:output_type -> spqr.ListShardsReply
	7, // 9: spqr.ShardService.AddDataShard:output_type -> google.protobuf.Empty
	7, // 10: spqr.ShardService.AddWorldShard:output_type -> google.protobuf.Empty
	2, // 11: spqr.ShardService.GetShard:output_type -> spqr.ShardReply
	8, // [8:12] is the sub-list for method output_type
	4, // [4:8] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_protos_shard_proto_init() }
func file_protos_shard_proto_init() {
	if File_protos_shard_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_protos_shard_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Shard); i {
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
		file_protos_shard_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ShardInfo); i {
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
		file_protos_shard_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ShardReply); i {
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
		file_protos_shard_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ShardRequest); i {
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
		file_protos_shard_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListShardsReply); i {
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
		file_protos_shard_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AddShardRequest); i {
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
		file_protos_shard_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AddWorldShardRequest); i {
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
			RawDescriptor: file_protos_shard_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_protos_shard_proto_goTypes,
		DependencyIndexes: file_protos_shard_proto_depIdxs,
		MessageInfos:      file_protos_shard_proto_msgTypes,
	}.Build()
	File_protos_shard_proto = out.File
	file_protos_shard_proto_rawDesc = nil
	file_protos_shard_proto_goTypes = nil
	file_protos_shard_proto_depIdxs = nil
}
