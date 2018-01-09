// Code generated by protoc-gen-go. DO NOT EDIT.
// source: hub.proto

/*
Package hub is a generated protocol buffer package.

It is generated from these files:
	hub.proto

It has these top-level messages:
	PutRequest
	PutResponse
	GetRequest
	GetResponse
	Message
*/
package hub

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type PutRequest struct {
	ClientID   string     `protobuf:"bytes,1,opt,name=ClientID" json:"ClientID,omitempty"`
	JournalDir string     `protobuf:"bytes,2,opt,name=JournalDir" json:"JournalDir,omitempty"`
	Messages   []*Message `protobuf:"bytes,3,rep,name=Messages" json:"Messages,omitempty"`
}

func (m *PutRequest) Reset()                    { *m = PutRequest{} }
func (m *PutRequest) String() string            { return proto.CompactTextString(m) }
func (*PutRequest) ProtoMessage()               {}
func (*PutRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *PutRequest) GetClientID() string {
	if m != nil {
		return m.ClientID
	}
	return ""
}

func (m *PutRequest) GetJournalDir() string {
	if m != nil {
		return m.JournalDir
	}
	return ""
}

func (m *PutRequest) GetMessages() []*Message {
	if m != nil {
		return m.Messages
	}
	return nil
}

type PutResponse struct {
}

func (m *PutResponse) Reset()                    { *m = PutResponse{} }
func (m *PutResponse) String() string            { return proto.CompactTextString(m) }
func (*PutResponse) ProtoMessage()               {}
func (*PutResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

type GetRequest struct {
	ClientID   string `protobuf:"bytes,1,opt,name=ClientID" json:"ClientID,omitempty"`
	JournalDir string `protobuf:"bytes,2,opt,name=JournalDir" json:"JournalDir,omitempty"`
	Offset     uint64 `protobuf:"varint,3,opt,name=Offset" json:"Offset,omitempty"`
}

func (m *GetRequest) Reset()                    { *m = GetRequest{} }
func (m *GetRequest) String() string            { return proto.CompactTextString(m) }
func (*GetRequest) ProtoMessage()               {}
func (*GetRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *GetRequest) GetClientID() string {
	if m != nil {
		return m.ClientID
	}
	return ""
}

func (m *GetRequest) GetJournalDir() string {
	if m != nil {
		return m.JournalDir
	}
	return ""
}

func (m *GetRequest) GetOffset() uint64 {
	if m != nil {
		return m.Offset
	}
	return 0
}

type GetResponse struct {
	Messages []*Message `protobuf:"bytes,1,rep,name=Messages" json:"Messages,omitempty"`
}

func (m *GetResponse) Reset()                    { *m = GetResponse{} }
func (m *GetResponse) String() string            { return proto.CompactTextString(m) }
func (*GetResponse) ProtoMessage()               {}
func (*GetResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

func (m *GetResponse) GetMessages() []*Message {
	if m != nil {
		return m.Messages
	}
	return nil
}

type Message struct {
	Offset    uint64 `protobuf:"varint,1,opt,name=Offset" json:"Offset,omitempty"`
	Timestamp int64  `protobuf:"varint,2,opt,name=Timestamp" json:"Timestamp,omitempty"`
	Type      uint32 `protobuf:"varint,3,opt,name=Type" json:"Type,omitempty"`
	Key       []byte `protobuf:"bytes,4,opt,name=Key,proto3" json:"Key,omitempty"`
	Value     []byte `protobuf:"bytes,5,opt,name=Value,proto3" json:"Value,omitempty"`
}

func (m *Message) Reset()                    { *m = Message{} }
func (m *Message) String() string            { return proto.CompactTextString(m) }
func (*Message) ProtoMessage()               {}
func (*Message) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

func (m *Message) GetOffset() uint64 {
	if m != nil {
		return m.Offset
	}
	return 0
}

func (m *Message) GetTimestamp() int64 {
	if m != nil {
		return m.Timestamp
	}
	return 0
}

func (m *Message) GetType() uint32 {
	if m != nil {
		return m.Type
	}
	return 0
}

func (m *Message) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *Message) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func init() {
	proto.RegisterType((*PutRequest)(nil), "hub.PutRequest")
	proto.RegisterType((*PutResponse)(nil), "hub.PutResponse")
	proto.RegisterType((*GetRequest)(nil), "hub.GetRequest")
	proto.RegisterType((*GetResponse)(nil), "hub.GetResponse")
	proto.RegisterType((*Message)(nil), "hub.Message")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for Hub service

type HubClient interface {
	Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error)
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
}

type hubClient struct {
	cc *grpc.ClientConn
}

func NewHubClient(cc *grpc.ClientConn) HubClient {
	return &hubClient{cc}
}

func (c *hubClient) Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error) {
	out := new(PutResponse)
	err := grpc.Invoke(ctx, "/hub.Hub/Put", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *hubClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := grpc.Invoke(ctx, "/hub.Hub/Get", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for Hub service

type HubServer interface {
	Put(context.Context, *PutRequest) (*PutResponse, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
}

func RegisterHubServer(s *grpc.Server, srv HubServer) {
	s.RegisterService(&_Hub_serviceDesc, srv)
}

func _Hub_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(HubServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/hub.Hub/Put",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(HubServer).Put(ctx, req.(*PutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Hub_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(HubServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/hub.Hub/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(HubServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Hub_serviceDesc = grpc.ServiceDesc{
	ServiceName: "hub.Hub",
	HandlerType: (*HubServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Put",
			Handler:    _Hub_Put_Handler,
		},
		{
			MethodName: "Get",
			Handler:    _Hub_Get_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "hub.proto",
}

func init() { proto.RegisterFile("hub.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 288 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x92, 0x4f, 0x4b, 0xc3, 0x40,
	0x10, 0xc5, 0xbb, 0x6e, 0x5a, 0x9b, 0x49, 0x8b, 0x65, 0x10, 0x59, 0x8a, 0x48, 0xc8, 0x29, 0x78,
	0xe8, 0xa1, 0x1e, 0xfc, 0x00, 0x16, 0xea, 0x1f, 0xc4, 0xb2, 0x14, 0x6f, 0x82, 0x09, 0x4c, 0x6d,
	0x20, 0x4d, 0x62, 0x76, 0xf7, 0x50, 0xfc, 0xf2, 0x92, 0xed, 0xda, 0xc4, 0x8b, 0x27, 0x6f, 0x33,
	0x6f, 0x86, 0x79, 0x3f, 0xde, 0x2e, 0xf8, 0x5b, 0x93, 0xce, 0xaa, 0xba, 0xd4, 0x25, 0xf2, 0xad,
	0x49, 0xa3, 0x1a, 0x60, 0x65, 0xb4, 0xa4, 0x4f, 0x43, 0x4a, 0xe3, 0x14, 0x86, 0x77, 0x79, 0x46,
	0x85, 0x7e, 0x58, 0x08, 0x16, 0xb2, 0xd8, 0x97, 0xc7, 0x1e, 0xaf, 0x00, 0x1e, 0x4b, 0x53, 0x17,
	0x49, 0xbe, 0xc8, 0x6a, 0x71, 0x62, 0xa7, 0x1d, 0x05, 0x63, 0x18, 0x3e, 0x93, 0x52, 0xc9, 0x07,
	0x29, 0xc1, 0x43, 0x1e, 0x07, 0xf3, 0xd1, 0xac, 0x31, 0x73, 0xa2, 0x3c, 0x4e, 0xa3, 0x31, 0x04,
	0xd6, 0x53, 0x55, 0x65, 0xa1, 0x28, 0x7a, 0x07, 0x58, 0xd2, 0xbf, 0x20, 0x5c, 0xc0, 0xe0, 0x65,
	0xb3, 0x51, 0xa4, 0x05, 0x0f, 0x59, 0xec, 0x49, 0xd7, 0x45, 0xb7, 0x10, 0x58, 0x87, 0x83, 0xe1,
	0x2f, 0x52, 0xf6, 0x27, 0xe9, 0x17, 0x9c, 0xba, 0xba, 0x73, 0x9b, 0x75, 0x6f, 0xe3, 0x25, 0xf8,
	0xeb, 0x6c, 0x47, 0x4a, 0x27, 0xbb, 0xca, 0x22, 0x71, 0xd9, 0x0a, 0x88, 0xe0, 0xad, 0xf7, 0x15,
	0x59, 0x9e, 0xb1, 0xb4, 0x35, 0x4e, 0x80, 0x3f, 0xd1, 0x5e, 0x78, 0x21, 0x8b, 0x47, 0xb2, 0x29,
	0xf1, 0x1c, 0xfa, 0xaf, 0x49, 0x6e, 0x48, 0xf4, 0xad, 0x76, 0x68, 0xe6, 0x6f, 0xc0, 0xef, 0x4d,
	0x8a, 0xd7, 0xc0, 0x57, 0x46, 0xe3, 0x99, 0x45, 0x6c, 0xdf, 0x6a, 0x3a, 0x69, 0x05, 0x17, 0x64,
	0xaf, 0xd9, 0x5d, 0xd2, 0xcf, 0x6e, 0x1b, 0xaa, 0xdb, 0xed, 0x64, 0x10, 0xf5, 0xd2, 0x81, 0xfd,
	0x05, 0x37, 0xdf, 0x01, 0x00, 0x00, 0xff, 0xff, 0x9b, 0x24, 0x2b, 0x8b, 0x12, 0x02, 0x00, 0x00,
}