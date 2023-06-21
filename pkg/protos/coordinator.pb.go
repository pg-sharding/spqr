// Code generated by protoc-gen-go. DO NOT EDIT.
// source: protos/coordinator.proto

package proto

import (
	fmt "fmt"
	math "math"

	proto "github.com/golang/protobuf/proto"
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

type OpenRouterRequest struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *OpenRouterRequest) Reset()         { *m = OpenRouterRequest{} }
func (m *OpenRouterRequest) String() string { return proto.CompactTextString(m) }
func (*OpenRouterRequest) ProtoMessage()    {}
func (*OpenRouterRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f8f560d2440f681e, []int{0}
}

func (m *OpenRouterRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_OpenRouterRequest.Unmarshal(m, b)
}
func (m *OpenRouterRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_OpenRouterRequest.Marshal(b, m, deterministic)
}
func (m *OpenRouterRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_OpenRouterRequest.Merge(m, src)
}
func (m *OpenRouterRequest) XXX_Size() int {
	return xxx_messageInfo_OpenRouterRequest.Size(m)
}
func (m *OpenRouterRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_OpenRouterRequest.DiscardUnknown(m)
}

var xxx_messageInfo_OpenRouterRequest proto.InternalMessageInfo

type OpenRouterReply struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *OpenRouterReply) Reset()         { *m = OpenRouterReply{} }
func (m *OpenRouterReply) String() string { return proto.CompactTextString(m) }
func (*OpenRouterReply) ProtoMessage()    {}
func (*OpenRouterReply) Descriptor() ([]byte, []int) {
	return fileDescriptor_f8f560d2440f681e, []int{1}
}

func (m *OpenRouterReply) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_OpenRouterReply.Unmarshal(m, b)
}
func (m *OpenRouterReply) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_OpenRouterReply.Marshal(b, m, deterministic)
}
func (m *OpenRouterReply) XXX_Merge(src proto.Message) {
	xxx_messageInfo_OpenRouterReply.Merge(m, src)
}
func (m *OpenRouterReply) XXX_Size() int {
	return xxx_messageInfo_OpenRouterReply.Size(m)
}
func (m *OpenRouterReply) XXX_DiscardUnknown() {
	xxx_messageInfo_OpenRouterReply.DiscardUnknown(m)
}

var xxx_messageInfo_OpenRouterReply proto.InternalMessageInfo

type CloseRouterRequest struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CloseRouterRequest) Reset()         { *m = CloseRouterRequest{} }
func (m *CloseRouterRequest) String() string { return proto.CompactTextString(m) }
func (*CloseRouterRequest) ProtoMessage()    {}
func (*CloseRouterRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f8f560d2440f681e, []int{2}
}

func (m *CloseRouterRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CloseRouterRequest.Unmarshal(m, b)
}
func (m *CloseRouterRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CloseRouterRequest.Marshal(b, m, deterministic)
}
func (m *CloseRouterRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CloseRouterRequest.Merge(m, src)
}
func (m *CloseRouterRequest) XXX_Size() int {
	return xxx_messageInfo_CloseRouterRequest.Size(m)
}
func (m *CloseRouterRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_CloseRouterRequest.DiscardUnknown(m)
}

var xxx_messageInfo_CloseRouterRequest proto.InternalMessageInfo

type CloseRouterReply struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CloseRouterReply) Reset()         { *m = CloseRouterReply{} }
func (m *CloseRouterReply) String() string { return proto.CompactTextString(m) }
func (*CloseRouterReply) ProtoMessage()    {}
func (*CloseRouterReply) Descriptor() ([]byte, []int) {
	return fileDescriptor_f8f560d2440f681e, []int{3}
}

func (m *CloseRouterReply) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CloseRouterReply.Unmarshal(m, b)
}
func (m *CloseRouterReply) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CloseRouterReply.Marshal(b, m, deterministic)
}
func (m *CloseRouterReply) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CloseRouterReply.Merge(m, src)
}
func (m *CloseRouterReply) XXX_Size() int {
	return xxx_messageInfo_CloseRouterReply.Size(m)
}
func (m *CloseRouterReply) XXX_DiscardUnknown() {
	xxx_messageInfo_CloseRouterReply.DiscardUnknown(m)
}

var xxx_messageInfo_CloseRouterReply proto.InternalMessageInfo

type GetRouterStatusRequest struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *GetRouterStatusRequest) Reset()         { *m = GetRouterStatusRequest{} }
func (m *GetRouterStatusRequest) String() string { return proto.CompactTextString(m) }
func (*GetRouterStatusRequest) ProtoMessage()    {}
func (*GetRouterStatusRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f8f560d2440f681e, []int{4}
}

func (m *GetRouterStatusRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetRouterStatusRequest.Unmarshal(m, b)
}
func (m *GetRouterStatusRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetRouterStatusRequest.Marshal(b, m, deterministic)
}
func (m *GetRouterStatusRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetRouterStatusRequest.Merge(m, src)
}
func (m *GetRouterStatusRequest) XXX_Size() int {
	return xxx_messageInfo_GetRouterStatusRequest.Size(m)
}
func (m *GetRouterStatusRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_GetRouterStatusRequest.DiscardUnknown(m)
}

var xxx_messageInfo_GetRouterStatusRequest proto.InternalMessageInfo

type GetRouterStatusReply struct {
	Status               RouterStatus `protobuf:"varint,1,opt,name=status,proto3,enum=spqr.RouterStatus" json:"status,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *GetRouterStatusReply) Reset()         { *m = GetRouterStatusReply{} }
func (m *GetRouterStatusReply) String() string { return proto.CompactTextString(m) }
func (*GetRouterStatusReply) ProtoMessage()    {}
func (*GetRouterStatusReply) Descriptor() ([]byte, []int) {
	return fileDescriptor_f8f560d2440f681e, []int{5}
}

func (m *GetRouterStatusReply) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetRouterStatusReply.Unmarshal(m, b)
}
func (m *GetRouterStatusReply) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetRouterStatusReply.Marshal(b, m, deterministic)
}
func (m *GetRouterStatusReply) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetRouterStatusReply.Merge(m, src)
}
func (m *GetRouterStatusReply) XXX_Size() int {
	return xxx_messageInfo_GetRouterStatusReply.Size(m)
}
func (m *GetRouterStatusReply) XXX_DiscardUnknown() {
	xxx_messageInfo_GetRouterStatusReply.DiscardUnknown(m)
}

var xxx_messageInfo_GetRouterStatusReply proto.InternalMessageInfo

func (m *GetRouterStatusReply) GetStatus() RouterStatus {
	if m != nil {
		return m.Status
	}
	return RouterStatus_CLOSED
}

func init() {
	proto.RegisterType((*OpenRouterRequest)(nil), "spqr.OpenRouterRequest")
	proto.RegisterType((*OpenRouterReply)(nil), "spqr.OpenRouterReply")
	proto.RegisterType((*CloseRouterRequest)(nil), "spqr.CloseRouterRequest")
	proto.RegisterType((*CloseRouterReply)(nil), "spqr.CloseRouterReply")
	proto.RegisterType((*GetRouterStatusRequest)(nil), "spqr.GetRouterStatusRequest")
	proto.RegisterType((*GetRouterStatusReply)(nil), "spqr.GetRouterStatusReply")
}

func init() { proto.RegisterFile("protos/coordinator.proto", fileDescriptor_f8f560d2440f681e) }

var fileDescriptor_f8f560d2440f681e = []byte{
	// 247 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x74, 0x91, 0xc1, 0x4a, 0xc3, 0x40,
	0x10, 0x86, 0x0d, 0x48, 0x0f, 0xa3, 0x18, 0x3b, 0xad, 0x75, 0x59, 0x3c, 0x48, 0x4e, 0xe2, 0x21,
	0x85, 0x7a, 0x17, 0xac, 0x07, 0x4f, 0x22, 0xb4, 0x9e, 0xbc, 0xd5, 0x3a, 0x48, 0x61, 0xe9, 0x6c,
	0x76, 0x37, 0x42, 0x9e, 0xd7, 0x17, 0x91, 0xdd, 0x8d, 0x98, 0xb8, 0xe9, 0x29, 0xe4, 0xff, 0xbe,
	0x0c, 0xff, 0x4c, 0x40, 0x68, 0xc3, 0x8e, 0xed, 0x7c, 0xcb, 0x6c, 0x3e, 0x76, 0xfb, 0x8d, 0x63,
	0x53, 0x86, 0x08, 0x8f, 0xad, 0xae, 0x8c, 0x9c, 0xb4, 0xdc, 0x70, 0xed, 0xa8, 0x45, 0xc5, 0x04,
	0xc6, 0x2f, 0x9a, 0xf6, 0xab, 0x90, 0xad, 0xa8, 0xaa, 0xc9, 0xba, 0x62, 0x0c, 0x79, 0x37, 0xd4,
	0xaa, 0x29, 0xa6, 0x80, 0x8f, 0x8a, 0x2d, 0xf5, 0x45, 0x84, 0xf3, 0x5e, 0xea, 0x4d, 0x01, 0xb3,
	0x27, 0x72, 0x31, 0x59, 0xbb, 0x8d, 0xab, 0xed, 0xaf, 0xbd, 0x84, 0x69, 0x42, 0xb4, 0x6a, 0xf0,
	0x16, 0x46, 0x36, 0xbc, 0x8a, 0xec, 0x3a, 0xbb, 0x39, 0x5b, 0x60, 0xe9, 0xfb, 0x96, 0x3d, 0xb1,
	0x35, 0x16, 0xdf, 0x19, 0xe4, 0xaf, 0xac, 0x59, 0xf1, 0x67, 0xb3, 0x26, 0xf3, 0xb5, 0xdb, 0x12,
	0xde, 0x03, 0xfc, 0xd5, 0xc5, 0xcb, 0xf8, 0x75, 0xb2, 0x95, 0xbc, 0x48, 0x81, 0xef, 0x7b, 0x84,
	0xcf, 0x90, 0xff, 0xeb, 0x85, 0x57, 0xd1, 0x1d, 0x5e, 0x44, 0xca, 0x03, 0x34, 0x8e, 0x7b, 0x80,
	0x93, 0xce, 0x51, 0x50, 0x44, 0x39, 0xbd, 0x9e, 0x9c, 0x0d, 0x90, 0x30, 0x62, 0x79, 0xfa, 0x06,
	0x1e, 0xcd, 0xc3, 0x3f, 0x7a, 0x1f, 0x85, 0xc7, 0xdd, 0x4f, 0x00, 0x00, 0x00, 0xff, 0xff, 0x09,
	0x2a, 0x4f, 0x86, 0xe1, 0x01, 0x00, 0x00,
}
