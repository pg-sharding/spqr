// Code generated by protoc-gen-go. DO NOT EDIT.
// source: protos/sharding_rules.proto

package proto

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type ShardingRuleEntry struct {
	TableName            string   `protobuf:"bytes,1,opt,name=tableName,proto3" json:"tableName,omitempty"`
	Column               string   `protobuf:"bytes,2,opt,name=column,proto3" json:"column,omitempty"`
	HashFunction         string   `protobuf:"bytes,3,opt,name=hashFunction,proto3" json:"hashFunction,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ShardingRuleEntry) Reset()         { *m = ShardingRuleEntry{} }
func (m *ShardingRuleEntry) String() string { return proto.CompactTextString(m) }
func (*ShardingRuleEntry) ProtoMessage()    {}
func (*ShardingRuleEntry) Descriptor() ([]byte, []int) {
	return fileDescriptor_b2011e1e95c68234, []int{0}
}

func (m *ShardingRuleEntry) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ShardingRuleEntry.Unmarshal(m, b)
}
func (m *ShardingRuleEntry) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ShardingRuleEntry.Marshal(b, m, deterministic)
}
func (m *ShardingRuleEntry) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ShardingRuleEntry.Merge(m, src)
}
func (m *ShardingRuleEntry) XXX_Size() int {
	return xxx_messageInfo_ShardingRuleEntry.Size(m)
}
func (m *ShardingRuleEntry) XXX_DiscardUnknown() {
	xxx_messageInfo_ShardingRuleEntry.DiscardUnknown(m)
}

var xxx_messageInfo_ShardingRuleEntry proto.InternalMessageInfo

func (m *ShardingRuleEntry) GetTableName() string {
	if m != nil {
		return m.TableName
	}
	return ""
}

func (m *ShardingRuleEntry) GetColumn() string {
	if m != nil {
		return m.Column
	}
	return ""
}

func (m *ShardingRuleEntry) GetHashFunction() string {
	if m != nil {
		return m.HashFunction
	}
	return ""
}

type ShardingRule struct {
	Id                   string               `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	ShardingRuleEntry    []*ShardingRuleEntry `protobuf:"bytes,2,rep,name=ShardingRuleEntry,proto3" json:"ShardingRuleEntry,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *ShardingRule) Reset()         { *m = ShardingRule{} }
func (m *ShardingRule) String() string { return proto.CompactTextString(m) }
func (*ShardingRule) ProtoMessage()    {}
func (*ShardingRule) Descriptor() ([]byte, []int) {
	return fileDescriptor_b2011e1e95c68234, []int{1}
}

func (m *ShardingRule) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ShardingRule.Unmarshal(m, b)
}
func (m *ShardingRule) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ShardingRule.Marshal(b, m, deterministic)
}
func (m *ShardingRule) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ShardingRule.Merge(m, src)
}
func (m *ShardingRule) XXX_Size() int {
	return xxx_messageInfo_ShardingRule.Size(m)
}
func (m *ShardingRule) XXX_DiscardUnknown() {
	xxx_messageInfo_ShardingRule.DiscardUnknown(m)
}

var xxx_messageInfo_ShardingRule proto.InternalMessageInfo

func (m *ShardingRule) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *ShardingRule) GetShardingRuleEntry() []*ShardingRuleEntry {
	if m != nil {
		return m.ShardingRuleEntry
	}
	return nil
}

type AddShardingRuleRequest struct {
	Rules                []*ShardingRule `protobuf:"bytes,1,rep,name=rules,proto3" json:"rules,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AddShardingRuleRequest) Reset()         { *m = AddShardingRuleRequest{} }
func (m *AddShardingRuleRequest) String() string { return proto.CompactTextString(m) }
func (*AddShardingRuleRequest) ProtoMessage()    {}
func (*AddShardingRuleRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_b2011e1e95c68234, []int{2}
}

func (m *AddShardingRuleRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_AddShardingRuleRequest.Unmarshal(m, b)
}
func (m *AddShardingRuleRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_AddShardingRuleRequest.Marshal(b, m, deterministic)
}
func (m *AddShardingRuleRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AddShardingRuleRequest.Merge(m, src)
}
func (m *AddShardingRuleRequest) XXX_Size() int {
	return xxx_messageInfo_AddShardingRuleRequest.Size(m)
}
func (m *AddShardingRuleRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_AddShardingRuleRequest.DiscardUnknown(m)
}

var xxx_messageInfo_AddShardingRuleRequest proto.InternalMessageInfo

func (m *AddShardingRuleRequest) GetRules() []*ShardingRule {
	if m != nil {
		return m.Rules
	}
	return nil
}

type AddShardingRuleReply struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *AddShardingRuleReply) Reset()         { *m = AddShardingRuleReply{} }
func (m *AddShardingRuleReply) String() string { return proto.CompactTextString(m) }
func (*AddShardingRuleReply) ProtoMessage()    {}
func (*AddShardingRuleReply) Descriptor() ([]byte, []int) {
	return fileDescriptor_b2011e1e95c68234, []int{3}
}

func (m *AddShardingRuleReply) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_AddShardingRuleReply.Unmarshal(m, b)
}
func (m *AddShardingRuleReply) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_AddShardingRuleReply.Marshal(b, m, deterministic)
}
func (m *AddShardingRuleReply) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AddShardingRuleReply.Merge(m, src)
}
func (m *AddShardingRuleReply) XXX_Size() int {
	return xxx_messageInfo_AddShardingRuleReply.Size(m)
}
func (m *AddShardingRuleReply) XXX_DiscardUnknown() {
	xxx_messageInfo_AddShardingRuleReply.DiscardUnknown(m)
}

var xxx_messageInfo_AddShardingRuleReply proto.InternalMessageInfo

type ListShardingRuleRequest struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ListShardingRuleRequest) Reset()         { *m = ListShardingRuleRequest{} }
func (m *ListShardingRuleRequest) String() string { return proto.CompactTextString(m) }
func (*ListShardingRuleRequest) ProtoMessage()    {}
func (*ListShardingRuleRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_b2011e1e95c68234, []int{4}
}

func (m *ListShardingRuleRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ListShardingRuleRequest.Unmarshal(m, b)
}
func (m *ListShardingRuleRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ListShardingRuleRequest.Marshal(b, m, deterministic)
}
func (m *ListShardingRuleRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ListShardingRuleRequest.Merge(m, src)
}
func (m *ListShardingRuleRequest) XXX_Size() int {
	return xxx_messageInfo_ListShardingRuleRequest.Size(m)
}
func (m *ListShardingRuleRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ListShardingRuleRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ListShardingRuleRequest proto.InternalMessageInfo

type ListShardingRuleReply struct {
	Rules                []*ShardingRule `protobuf:"bytes,1,rep,name=rules,proto3" json:"rules,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *ListShardingRuleReply) Reset()         { *m = ListShardingRuleReply{} }
func (m *ListShardingRuleReply) String() string { return proto.CompactTextString(m) }
func (*ListShardingRuleReply) ProtoMessage()    {}
func (*ListShardingRuleReply) Descriptor() ([]byte, []int) {
	return fileDescriptor_b2011e1e95c68234, []int{5}
}

func (m *ListShardingRuleReply) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ListShardingRuleReply.Unmarshal(m, b)
}
func (m *ListShardingRuleReply) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ListShardingRuleReply.Marshal(b, m, deterministic)
}
func (m *ListShardingRuleReply) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ListShardingRuleReply.Merge(m, src)
}
func (m *ListShardingRuleReply) XXX_Size() int {
	return xxx_messageInfo_ListShardingRuleReply.Size(m)
}
func (m *ListShardingRuleReply) XXX_DiscardUnknown() {
	xxx_messageInfo_ListShardingRuleReply.DiscardUnknown(m)
}

var xxx_messageInfo_ListShardingRuleReply proto.InternalMessageInfo

func (m *ListShardingRuleReply) GetRules() []*ShardingRule {
	if m != nil {
		return m.Rules
	}
	return nil
}

type DropShardingRuleRequest struct {
	Id                   []string `protobuf:"bytes,1,rep,name=id,proto3" json:"id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *DropShardingRuleRequest) Reset()         { *m = DropShardingRuleRequest{} }
func (m *DropShardingRuleRequest) String() string { return proto.CompactTextString(m) }
func (*DropShardingRuleRequest) ProtoMessage()    {}
func (*DropShardingRuleRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_b2011e1e95c68234, []int{6}
}

func (m *DropShardingRuleRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DropShardingRuleRequest.Unmarshal(m, b)
}
func (m *DropShardingRuleRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DropShardingRuleRequest.Marshal(b, m, deterministic)
}
func (m *DropShardingRuleRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DropShardingRuleRequest.Merge(m, src)
}
func (m *DropShardingRuleRequest) XXX_Size() int {
	return xxx_messageInfo_DropShardingRuleRequest.Size(m)
}
func (m *DropShardingRuleRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_DropShardingRuleRequest.DiscardUnknown(m)
}

var xxx_messageInfo_DropShardingRuleRequest proto.InternalMessageInfo

func (m *DropShardingRuleRequest) GetId() []string {
	if m != nil {
		return m.Id
	}
	return nil
}

type DropShardingRuleReply struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *DropShardingRuleReply) Reset()         { *m = DropShardingRuleReply{} }
func (m *DropShardingRuleReply) String() string { return proto.CompactTextString(m) }
func (*DropShardingRuleReply) ProtoMessage()    {}
func (*DropShardingRuleReply) Descriptor() ([]byte, []int) {
	return fileDescriptor_b2011e1e95c68234, []int{7}
}

func (m *DropShardingRuleReply) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DropShardingRuleReply.Unmarshal(m, b)
}
func (m *DropShardingRuleReply) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DropShardingRuleReply.Marshal(b, m, deterministic)
}
func (m *DropShardingRuleReply) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DropShardingRuleReply.Merge(m, src)
}
func (m *DropShardingRuleReply) XXX_Size() int {
	return xxx_messageInfo_DropShardingRuleReply.Size(m)
}
func (m *DropShardingRuleReply) XXX_DiscardUnknown() {
	xxx_messageInfo_DropShardingRuleReply.DiscardUnknown(m)
}

var xxx_messageInfo_DropShardingRuleReply proto.InternalMessageInfo

func init() {
	proto.RegisterType((*ShardingRuleEntry)(nil), "spqr.ShardingRuleEntry")
	proto.RegisterType((*ShardingRule)(nil), "spqr.ShardingRule")
	proto.RegisterType((*AddShardingRuleRequest)(nil), "spqr.AddShardingRuleRequest")
	proto.RegisterType((*AddShardingRuleReply)(nil), "spqr.AddShardingRuleReply")
	proto.RegisterType((*ListShardingRuleRequest)(nil), "spqr.ListShardingRuleRequest")
	proto.RegisterType((*ListShardingRuleReply)(nil), "spqr.ListShardingRuleReply")
	proto.RegisterType((*DropShardingRuleRequest)(nil), "spqr.DropShardingRuleRequest")
	proto.RegisterType((*DropShardingRuleReply)(nil), "spqr.DropShardingRuleReply")
}

func init() { proto.RegisterFile("protos/sharding_rules.proto", fileDescriptor_b2011e1e95c68234) }

var fileDescriptor_b2011e1e95c68234 = []byte{
	// 330 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x92, 0x51, 0x6b, 0xc2, 0x30,
	0x14, 0x85, 0xb5, 0x6e, 0x82, 0x77, 0x32, 0xe6, 0x45, 0x6d, 0xa7, 0x0e, 0x24, 0x4f, 0xee, 0x45,
	0xc1, 0xfd, 0x02, 0x65, 0xee, 0x69, 0x08, 0xab, 0x6f, 0x7b, 0x19, 0xd5, 0x86, 0x19, 0x88, 0x6d,
	0x4c, 0xd2, 0x41, 0xff, 0xc3, 0x7e, 0xf4, 0x30, 0x55, 0xe7, 0x9a, 0xf6, 0x61, 0x4f, 0xa5, 0xe7,
	0x24, 0xe7, 0x9e, 0xfb, 0x11, 0xe8, 0x0b, 0x19, 0xeb, 0x58, 0x4d, 0xd4, 0x36, 0x90, 0x21, 0x8b,
	0x3e, 0x3f, 0x64, 0xc2, 0xa9, 0x1a, 0x1b, 0x15, 0xaf, 0x94, 0xd8, 0x4b, 0xb2, 0x83, 0xd6, 0xea,
	0xe8, 0xfa, 0x09, 0xa7, 0x8b, 0x48, 0xcb, 0x14, 0x07, 0xd0, 0xd0, 0xc1, 0x9a, 0xd3, 0x65, 0xb0,
	0xa3, 0x5e, 0x75, 0x58, 0x1d, 0x35, 0xfc, 0x5f, 0x01, 0xbb, 0x50, 0xdf, 0xc4, 0x3c, 0xd9, 0x45,
	0x9e, 0x63, 0xac, 0xe3, 0x1f, 0x12, 0x68, 0x6e, 0x03, 0xb5, 0x7d, 0x49, 0xa2, 0x8d, 0x66, 0x71,
	0xe4, 0xd5, 0x8c, 0xfb, 0x47, 0x23, 0x14, 0x9a, 0x97, 0xe3, 0xf0, 0x16, 0x1c, 0x16, 0x1e, 0x47,
	0x38, 0x2c, 0xc4, 0x45, 0x41, 0x1d, 0xcf, 0x19, 0xd6, 0x46, 0x37, 0x53, 0x77, 0x7c, 0x28, 0x3c,
	0xb6, 0x6c, 0xdf, 0xbe, 0x41, 0xe6, 0xd0, 0x9d, 0x85, 0xe1, 0xa5, 0xee, 0xd3, 0x7d, 0x42, 0x95,
	0xc6, 0x11, 0x5c, 0x1b, 0x08, 0x5e, 0xd5, 0x84, 0xa2, 0x1d, 0xea, 0x67, 0x07, 0x48, 0x17, 0xda,
	0x56, 0x86, 0xe0, 0x29, 0xb9, 0x07, 0xf7, 0x95, 0x29, 0x5d, 0x10, 0x4e, 0x66, 0xd0, 0xb1, 0x2d,
	0xc1, 0xd3, 0x7f, 0x4c, 0x7d, 0x04, 0xf7, 0x59, 0xc6, 0xa2, 0xa8, 0xfa, 0x89, 0x55, 0x2d, 0x63,
	0x45, 0x5c, 0xe8, 0xd8, 0x47, 0x05, 0x4f, 0xa7, 0xdf, 0x0e, 0xb4, 0x2f, 0x55, 0xb5, 0xa2, 0xf2,
	0x8b, 0x6d, 0x28, 0x2e, 0xe1, 0x2e, 0xb7, 0x92, 0xc2, 0x41, 0xd6, 0xa5, 0x18, 0x57, 0xaf, 0x57,
	0xe2, 0x1e, 0x40, 0x54, 0xf0, 0x0d, 0x5a, 0xf9, 0x06, 0x0a, 0x1f, 0xb2, 0x2b, 0x25, 0x5b, 0xf4,
	0xfa, 0x65, 0xf6, 0x39, 0x32, 0x8f, 0xf0, 0x1c, 0x59, 0x82, 0xfd, 0x14, 0x59, 0x88, 0x9e, 0x54,
	0xe6, 0xcd, 0x77, 0x38, 0xf8, 0x13, 0xf3, 0xec, 0xd7, 0x75, 0xf3, 0x79, 0xfa, 0x09, 0x00, 0x00,
	0xff, 0xff, 0x52, 0x8a, 0x84, 0xd8, 0x1c, 0x03, 0x00, 0x00,
}
