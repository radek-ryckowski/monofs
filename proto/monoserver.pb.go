// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0-devel
// 	protoc        v3.12.4
// source: proto/monoserver.proto

package proto

import (
	empty "github.com/golang/protobuf/ptypes/empty"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
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

// Symbols defined in public import of google/protobuf/empty.proto.

type Empty = empty.Empty

// Symbols defined in public import of google/protobuf/timestamp.proto.

type Timestamp = timestamp.Timestamp

type StatRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Fs string `protobuf:"bytes,1,opt,name=fs,proto3" json:"fs,omitempty"`
}

func (x *StatRequest) Reset() {
	*x = StatRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_monoserver_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StatRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StatRequest) ProtoMessage() {}

func (x *StatRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_monoserver_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StatRequest.ProtoReflect.Descriptor instead.
func (*StatRequest) Descriptor() ([]byte, []int) {
	return file_proto_monoserver_proto_rawDescGZIP(), []int{0}
}

func (x *StatRequest) GetFs() string {
	if x != nil {
		return x.Fs
	}
	return ""
}

type StatResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id              string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	BlockSize       uint32 `protobuf:"varint,2,opt,name=BlockSize,proto3" json:"BlockSize,omitempty"`
	Blocks          uint64 `protobuf:"varint,3,opt,name=Blocks,proto3" json:"Blocks,omitempty"`
	BlocksFree      uint64 `protobuf:"varint,4,opt,name=BlocksFree,proto3" json:"BlocksFree,omitempty"`
	BlocksAvailable uint64 `protobuf:"varint,5,opt,name=BlocksAvailable,proto3" json:"BlocksAvailable,omitempty"`
}

func (x *StatResponse) Reset() {
	*x = StatResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_monoserver_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StatResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StatResponse) ProtoMessage() {}

func (x *StatResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_monoserver_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StatResponse.ProtoReflect.Descriptor instead.
func (*StatResponse) Descriptor() ([]byte, []int) {
	return file_proto_monoserver_proto_rawDescGZIP(), []int{1}
}

func (x *StatResponse) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *StatResponse) GetBlockSize() uint32 {
	if x != nil {
		return x.BlockSize
	}
	return 0
}

func (x *StatResponse) GetBlocks() uint64 {
	if x != nil {
		return x.Blocks
	}
	return 0
}

func (x *StatResponse) GetBlocksFree() uint64 {
	if x != nil {
		return x.BlocksFree
	}
	return 0
}

func (x *StatResponse) GetBlocksAvailable() uint64 {
	if x != nil {
		return x.BlocksAvailable
	}
	return 0
}

type File struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Bucket string `protobuf:"bytes,1,opt,name=bucket,proto3" json:"bucket,omitempty"`
	Name   string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Uid    string `protobuf:"bytes,3,opt,name=uid,proto3" json:"uid,omitempty"`
	Gid    string `protobuf:"bytes,4,opt,name=gid,proto3" json:"gid,omitempty"`
	Size   int64  `protobuf:"varint,5,opt,name=size,proto3" json:"size,omitempty"`
	Mtime  int64  `protobuf:"varint,6,opt,name=mtime,proto3" json:"mtime,omitempty"`
	Ctime  int64  `protobuf:"varint,7,opt,name=ctime,proto3" json:"ctime,omitempty"`
	Atime  int64  `protobuf:"varint,8,opt,name=atime,proto3" json:"atime,omitempty"`
	Mode   int32  `protobuf:"varint,9,opt,name=mode,proto3" json:"mode,omitempty"`
	Type   int32  `protobuf:"varint,10,opt,name=type,proto3" json:"type,omitempty"`
	Hash   string `protobuf:"bytes,11,opt,name=hash,proto3" json:"hash,omitempty"`
}

func (x *File) Reset() {
	*x = File{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_monoserver_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *File) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*File) ProtoMessage() {}

func (x *File) ProtoReflect() protoreflect.Message {
	mi := &file_proto_monoserver_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use File.ProtoReflect.Descriptor instead.
func (*File) Descriptor() ([]byte, []int) {
	return file_proto_monoserver_proto_rawDescGZIP(), []int{2}
}

func (x *File) GetBucket() string {
	if x != nil {
		return x.Bucket
	}
	return ""
}

func (x *File) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *File) GetUid() string {
	if x != nil {
		return x.Uid
	}
	return ""
}

func (x *File) GetGid() string {
	if x != nil {
		return x.Gid
	}
	return ""
}

func (x *File) GetSize() int64 {
	if x != nil {
		return x.Size
	}
	return 0
}

func (x *File) GetMtime() int64 {
	if x != nil {
		return x.Mtime
	}
	return 0
}

func (x *File) GetCtime() int64 {
	if x != nil {
		return x.Ctime
	}
	return 0
}

func (x *File) GetAtime() int64 {
	if x != nil {
		return x.Atime
	}
	return 0
}

func (x *File) GetMode() int32 {
	if x != nil {
		return x.Mode
	}
	return 0
}

func (x *File) GetType() int32 {
	if x != nil {
		return x.Type
	}
	return 0
}

func (x *File) GetHash() string {
	if x != nil {
		return x.Hash
	}
	return ""
}

type ListRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Fs     string `protobuf:"bytes,1,opt,name=fs,proto3" json:"fs,omitempty"`
	Bucket string `protobuf:"bytes,2,opt,name=bucket,proto3" json:"bucket,omitempty"`
}

func (x *ListRequest) Reset() {
	*x = ListRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_monoserver_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListRequest) ProtoMessage() {}

func (x *ListRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_monoserver_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListRequest.ProtoReflect.Descriptor instead.
func (*ListRequest) Descriptor() ([]byte, []int) {
	return file_proto_monoserver_proto_rawDescGZIP(), []int{3}
}

func (x *ListRequest) GetFs() string {
	if x != nil {
		return x.Fs
	}
	return ""
}

func (x *ListRequest) GetBucket() string {
	if x != nil {
		return x.Bucket
	}
	return ""
}

type ListResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Files []*File `protobuf:"bytes,1,rep,name=files,proto3" json:"files,omitempty"`
}

func (x *ListResponse) Reset() {
	*x = ListResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_monoserver_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListResponse) ProtoMessage() {}

func (x *ListResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_monoserver_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListResponse.ProtoReflect.Descriptor instead.
func (*ListResponse) Descriptor() ([]byte, []int) {
	return file_proto_monoserver_proto_rawDescGZIP(), []int{4}
}

func (x *ListResponse) GetFiles() []*File {
	if x != nil {
		return x.Files
	}
	return nil
}

type GetSnapshotRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CreationId uint64 `protobuf:"varint,1,opt,name=creation_id,json=creationId,proto3" json:"creation_id,omitempty"`
	Auth       string `protobuf:"bytes,2,opt,name=auth,proto3" json:"auth,omitempty"`
}

func (x *GetSnapshotRequest) Reset() {
	*x = GetSnapshotRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_monoserver_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetSnapshotRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetSnapshotRequest) ProtoMessage() {}

func (x *GetSnapshotRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_monoserver_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetSnapshotRequest.ProtoReflect.Descriptor instead.
func (*GetSnapshotRequest) Descriptor() ([]byte, []int) {
	return file_proto_monoserver_proto_rawDescGZIP(), []int{5}
}

func (x *GetSnapshotRequest) GetCreationId() uint64 {
	if x != nil {
		return x.CreationId
	}
	return 0
}

func (x *GetSnapshotRequest) GetAuth() string {
	if x != nil {
		return x.Auth
	}
	return ""
}

type GetSnapshotResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id      string               `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Fs      string               `protobuf:"bytes,2,opt,name=fs,proto3" json:"fs,omitempty"`
	Name    string               `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty"`
	Created *timestamp.Timestamp `protobuf:"bytes,4,opt,name=created,proto3" json:"created,omitempty"`
	Status  string               `protobuf:"bytes,5,opt,name=status,proto3" json:"status,omitempty"`
}

func (x *GetSnapshotResponse) Reset() {
	*x = GetSnapshotResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_monoserver_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetSnapshotResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetSnapshotResponse) ProtoMessage() {}

func (x *GetSnapshotResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_monoserver_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetSnapshotResponse.ProtoReflect.Descriptor instead.
func (*GetSnapshotResponse) Descriptor() ([]byte, []int) {
	return file_proto_monoserver_proto_rawDescGZIP(), []int{6}
}

func (x *GetSnapshotResponse) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *GetSnapshotResponse) GetFs() string {
	if x != nil {
		return x.Fs
	}
	return ""
}

func (x *GetSnapshotResponse) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *GetSnapshotResponse) GetCreated() *timestamp.Timestamp {
	if x != nil {
		return x.Created
	}
	return nil
}

func (x *GetSnapshotResponse) GetStatus() string {
	if x != nil {
		return x.Status
	}
	return ""
}

type CreateSnapshotRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Fs   string `protobuf:"bytes,1,opt,name=fs,proto3" json:"fs,omitempty"`
	Name string `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty"`
	Auth string `protobuf:"bytes,4,opt,name=auth,proto3" json:"auth,omitempty"`
}

func (x *CreateSnapshotRequest) Reset() {
	*x = CreateSnapshotRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_monoserver_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreateSnapshotRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateSnapshotRequest) ProtoMessage() {}

func (x *CreateSnapshotRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_monoserver_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateSnapshotRequest.ProtoReflect.Descriptor instead.
func (*CreateSnapshotRequest) Descriptor() ([]byte, []int) {
	return file_proto_monoserver_proto_rawDescGZIP(), []int{7}
}

func (x *CreateSnapshotRequest) GetFs() string {
	if x != nil {
		return x.Fs
	}
	return ""
}

func (x *CreateSnapshotRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *CreateSnapshotRequest) GetAuth() string {
	if x != nil {
		return x.Auth
	}
	return ""
}

type CreateSnapshotResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CreationId uint64 `protobuf:"varint,1,opt,name=creation_id,json=creationId,proto3" json:"creation_id,omitempty"`
}

func (x *CreateSnapshotResponse) Reset() {
	*x = CreateSnapshotResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_monoserver_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreateSnapshotResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateSnapshotResponse) ProtoMessage() {}

func (x *CreateSnapshotResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_monoserver_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateSnapshotResponse.ProtoReflect.Descriptor instead.
func (*CreateSnapshotResponse) Descriptor() ([]byte, []int) {
	return file_proto_monoserver_proto_rawDescGZIP(), []int{8}
}

func (x *CreateSnapshotResponse) GetCreationId() uint64 {
	if x != nil {
		return x.CreationId
	}
	return 0
}

type ListSnapshotsResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id      string               `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Fs      string               `protobuf:"bytes,2,opt,name=fs,proto3" json:"fs,omitempty"`
	Name    string               `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty"`
	Created *timestamp.Timestamp `protobuf:"bytes,4,opt,name=created,proto3" json:"created,omitempty"`
	Status  string               `protobuf:"bytes,5,opt,name=status,proto3" json:"status,omitempty"`
}

func (x *ListSnapshotsResponse) Reset() {
	*x = ListSnapshotsResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_monoserver_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListSnapshotsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListSnapshotsResponse) ProtoMessage() {}

func (x *ListSnapshotsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_monoserver_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListSnapshotsResponse.ProtoReflect.Descriptor instead.
func (*ListSnapshotsResponse) Descriptor() ([]byte, []int) {
	return file_proto_monoserver_proto_rawDescGZIP(), []int{9}
}

func (x *ListSnapshotsResponse) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *ListSnapshotsResponse) GetFs() string {
	if x != nil {
		return x.Fs
	}
	return ""
}

func (x *ListSnapshotsResponse) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *ListSnapshotsResponse) GetCreated() *timestamp.Timestamp {
	if x != nil {
		return x.Created
	}
	return nil
}

func (x *ListSnapshotsResponse) GetStatus() string {
	if x != nil {
		return x.Status
	}
	return ""
}

type DeleteSnapshotRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Fs   string `protobuf:"bytes,1,opt,name=fs,proto3" json:"fs,omitempty"`
	Id   string `protobuf:"bytes,2,opt,name=id,proto3" json:"id,omitempty"`
	Auth string `protobuf:"bytes,3,opt,name=auth,proto3" json:"auth,omitempty"`
}

func (x *DeleteSnapshotRequest) Reset() {
	*x = DeleteSnapshotRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_monoserver_proto_msgTypes[10]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteSnapshotRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteSnapshotRequest) ProtoMessage() {}

func (x *DeleteSnapshotRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_monoserver_proto_msgTypes[10]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteSnapshotRequest.ProtoReflect.Descriptor instead.
func (*DeleteSnapshotRequest) Descriptor() ([]byte, []int) {
	return file_proto_monoserver_proto_rawDescGZIP(), []int{10}
}

func (x *DeleteSnapshotRequest) GetFs() string {
	if x != nil {
		return x.Fs
	}
	return ""
}

func (x *DeleteSnapshotRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *DeleteSnapshotRequest) GetAuth() string {
	if x != nil {
		return x.Auth
	}
	return ""
}

type DeleteSnapshotResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id     string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Fs     string `protobuf:"bytes,2,opt,name=fs,proto3" json:"fs,omitempty"`
	Status string `protobuf:"bytes,3,opt,name=status,proto3" json:"status,omitempty"`
}

func (x *DeleteSnapshotResponse) Reset() {
	*x = DeleteSnapshotResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_monoserver_proto_msgTypes[11]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteSnapshotResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteSnapshotResponse) ProtoMessage() {}

func (x *DeleteSnapshotResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_monoserver_proto_msgTypes[11]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteSnapshotResponse.ProtoReflect.Descriptor instead.
func (*DeleteSnapshotResponse) Descriptor() ([]byte, []int) {
	return file_proto_monoserver_proto_rawDescGZIP(), []int{11}
}

func (x *DeleteSnapshotResponse) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *DeleteSnapshotResponse) GetFs() string {
	if x != nil {
		return x.Fs
	}
	return ""
}

func (x *DeleteSnapshotResponse) GetStatus() string {
	if x != nil {
		return x.Status
	}
	return ""
}

var File_proto_monoserver_proto protoreflect.FileDescriptor

var file_proto_monoserver_proto_rawDesc = []byte{
	0x0a, 0x16, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x6d, 0x6f, 0x6e, 0x6f, 0x73, 0x65, 0x72, 0x76,
	0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x1b, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2f, 0x65, 0x6d, 0x70, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1f, 0x67, 0x6f,
	0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69,
	0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x1d, 0x0a,
	0x0b, 0x53, 0x74, 0x61, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x0e, 0x0a, 0x02,
	0x66, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x66, 0x73, 0x22, 0x9e, 0x01, 0x0a,
	0x0c, 0x53, 0x74, 0x61, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a,
	0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x1c, 0x0a,
	0x09, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x53, 0x69, 0x7a, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0d,
	0x52, 0x09, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x53, 0x69, 0x7a, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x42,
	0x6c, 0x6f, 0x63, 0x6b, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x06, 0x42, 0x6c, 0x6f,
	0x63, 0x6b, 0x73, 0x12, 0x1e, 0x0a, 0x0a, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x73, 0x46, 0x72, 0x65,
	0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0a, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x73, 0x46,
	0x72, 0x65, 0x65, 0x12, 0x28, 0x0a, 0x0f, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x73, 0x41, 0x76, 0x61,
	0x69, 0x6c, 0x61, 0x62, 0x6c, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0f, 0x42, 0x6c,
	0x6f, 0x63, 0x6b, 0x73, 0x41, 0x76, 0x61, 0x69, 0x6c, 0x61, 0x62, 0x6c, 0x65, 0x22, 0xe8, 0x01,
	0x0a, 0x04, 0x46, 0x69, 0x6c, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74, 0x12, 0x12,
	0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61,
	0x6d, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x03, 0x75, 0x69, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x67, 0x69, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x67, 0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x73, 0x69, 0x7a, 0x65, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x04, 0x73, 0x69, 0x7a, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x6d, 0x74,
	0x69, 0x6d, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x6d, 0x74, 0x69, 0x6d, 0x65,
	0x12, 0x14, 0x0a, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x61, 0x74, 0x69, 0x6d, 0x65, 0x18,
	0x08, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x61, 0x74, 0x69, 0x6d, 0x65, 0x12, 0x12, 0x0a, 0x04,
	0x6d, 0x6f, 0x64, 0x65, 0x18, 0x09, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x6d, 0x6f, 0x64, 0x65,
	0x12, 0x12, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04,
	0x74, 0x79, 0x70, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x61, 0x73, 0x68, 0x18, 0x0b, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x04, 0x68, 0x61, 0x73, 0x68, 0x22, 0x35, 0x0a, 0x0b, 0x4c, 0x69, 0x73, 0x74,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x0e, 0x0a, 0x02, 0x66, 0x73, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x02, 0x66, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x62, 0x75, 0x63, 0x6b, 0x65,
	0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74, 0x22,
	0x31, 0x0a, 0x0c, 0x4c, 0x69, 0x73, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12,
	0x21, 0x0a, 0x05, 0x66, 0x69, 0x6c, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0b,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x46, 0x69, 0x6c, 0x65, 0x52, 0x05, 0x66, 0x69, 0x6c,
	0x65, 0x73, 0x22, 0x49, 0x0a, 0x12, 0x47, 0x65, 0x74, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f,
	0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1f, 0x0a, 0x0b, 0x63, 0x72, 0x65, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0a, 0x63,
	0x72, 0x65, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x61, 0x75, 0x74,
	0x68, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x61, 0x75, 0x74, 0x68, 0x22, 0x97, 0x01,
	0x0a, 0x13, 0x47, 0x65, 0x74, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x0e, 0x0a, 0x02, 0x66, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x02, 0x66, 0x73, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x34, 0x0a, 0x07, 0x63, 0x72, 0x65,
	0x61, 0x74, 0x65, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d,
	0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x07, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x12,
	0x16, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x22, 0x4f, 0x0a, 0x15, 0x43, 0x72, 0x65, 0x61, 0x74,
	0x65, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x0e, 0x0a, 0x02, 0x66, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x66, 0x73,
	0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x6e, 0x61, 0x6d, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x61, 0x75, 0x74, 0x68, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x04, 0x61, 0x75, 0x74, 0x68, 0x22, 0x39, 0x0a, 0x16, 0x43, 0x72, 0x65, 0x61,
	0x74, 0x65, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x1f, 0x0a, 0x0b, 0x63, 0x72, 0x65, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0a, 0x63, 0x72, 0x65, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x49, 0x64, 0x22, 0x99, 0x01, 0x0a, 0x15, 0x4c, 0x69, 0x73, 0x74, 0x53, 0x6e, 0x61, 0x70,
	0x73, 0x68, 0x6f, 0x74, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a,
	0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x0e, 0x0a,
	0x02, 0x66, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x66, 0x73, 0x12, 0x12, 0x0a,
	0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d,
	0x65, 0x12, 0x34, 0x0a, 0x07, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x07,
	0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75,
	0x73, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x22,
	0x4b, 0x0a, 0x15, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f,
	0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x0e, 0x0a, 0x02, 0x66, 0x73, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x66, 0x73, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x61, 0x75, 0x74, 0x68,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x61, 0x75, 0x74, 0x68, 0x22, 0x50, 0x0a, 0x16,
	0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x0e, 0x0a, 0x02, 0x66, 0x73, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x02, 0x66, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x32, 0x3f,
	0x0a, 0x0a, 0x4d, 0x6f, 0x6e, 0x6f, 0x66, 0x73, 0x53, 0x74, 0x61, 0x74, 0x12, 0x31, 0x0a, 0x04,
	0x53, 0x74, 0x61, 0x74, 0x12, 0x12, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x53, 0x74, 0x61,
	0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x13, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x2e, 0x53, 0x74, 0x61, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x32,
	0x42, 0x0a, 0x0b, 0x4d, 0x6f, 0x6e, 0x6f, 0x66, 0x73, 0x50, 0x72, 0x6f, 0x78, 0x79, 0x12, 0x33,
	0x0a, 0x04, 0x4c, 0x69, 0x73, 0x74, 0x12, 0x12, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x4c,
	0x69, 0x73, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x13, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x00, 0x30, 0x01, 0x32, 0xc4, 0x02, 0x0a, 0x0d, 0x4d, 0x6f, 0x6e, 0x6f, 0x66, 0x73, 0x4d, 0x61,
	0x6e, 0x61, 0x67, 0x65, 0x72, 0x12, 0x4f, 0x0a, 0x0e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x53,
	0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x12, 0x1c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e,
	0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1d, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x43, 0x72,
	0x65, 0x61, 0x74, 0x65, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x49, 0x0a, 0x0d, 0x4c, 0x69, 0x73, 0x74, 0x53, 0x6e,
	0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x73, 0x12, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x1a,
	0x1c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x53, 0x6e, 0x61, 0x70,
	0x73, 0x68, 0x6f, 0x74, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x30,
	0x01, 0x12, 0x4f, 0x0a, 0x0e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x53, 0x6e, 0x61, 0x70, 0x73,
	0x68, 0x6f, 0x74, 0x12, 0x1c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x44, 0x65, 0x6c, 0x65,
	0x74, 0x65, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x1d, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65,
	0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x22, 0x00, 0x12, 0x46, 0x0a, 0x0b, 0x47, 0x65, 0x74, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f,
	0x74, 0x12, 0x19, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x47, 0x65, 0x74, 0x53, 0x6e, 0x61,
	0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1a, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x47, 0x65, 0x74, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x2a, 0x5a, 0x28, 0x67, 0x69,
	0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x72, 0x61, 0x72, 0x79, 0x64, 0x7a, 0x75,
	0x2f, 0x67, 0x6d, 0x6f, 0x6e, 0x6f, 0x72, 0x65, 0x70, 0x6f, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x3b, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x00, 0x50, 0x01, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_proto_monoserver_proto_rawDescOnce sync.Once
	file_proto_monoserver_proto_rawDescData = file_proto_monoserver_proto_rawDesc
)

func file_proto_monoserver_proto_rawDescGZIP() []byte {
	file_proto_monoserver_proto_rawDescOnce.Do(func() {
		file_proto_monoserver_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_monoserver_proto_rawDescData)
	})
	return file_proto_monoserver_proto_rawDescData
}

var file_proto_monoserver_proto_msgTypes = make([]protoimpl.MessageInfo, 12)
var file_proto_monoserver_proto_goTypes = []interface{}{
	(*StatRequest)(nil),            // 0: proto.StatRequest
	(*StatResponse)(nil),           // 1: proto.StatResponse
	(*File)(nil),                   // 2: proto.File
	(*ListRequest)(nil),            // 3: proto.ListRequest
	(*ListResponse)(nil),           // 4: proto.ListResponse
	(*GetSnapshotRequest)(nil),     // 5: proto.GetSnapshotRequest
	(*GetSnapshotResponse)(nil),    // 6: proto.GetSnapshotResponse
	(*CreateSnapshotRequest)(nil),  // 7: proto.CreateSnapshotRequest
	(*CreateSnapshotResponse)(nil), // 8: proto.CreateSnapshotResponse
	(*ListSnapshotsResponse)(nil),  // 9: proto.ListSnapshotsResponse
	(*DeleteSnapshotRequest)(nil),  // 10: proto.DeleteSnapshotRequest
	(*DeleteSnapshotResponse)(nil), // 11: proto.DeleteSnapshotResponse
	(*timestamp.Timestamp)(nil),    // 12: google.protobuf.Timestamp
	(*empty.Empty)(nil),            // 13: google.protobuf.Empty
}
var file_proto_monoserver_proto_depIdxs = []int32{
	2,  // 0: proto.ListResponse.files:type_name -> proto.File
	12, // 1: proto.GetSnapshotResponse.created:type_name -> google.protobuf.Timestamp
	12, // 2: proto.ListSnapshotsResponse.created:type_name -> google.protobuf.Timestamp
	0,  // 3: proto.MonofsStat.Stat:input_type -> proto.StatRequest
	3,  // 4: proto.MonofsProxy.List:input_type -> proto.ListRequest
	7,  // 5: proto.MonofsManager.CreateSnapshot:input_type -> proto.CreateSnapshotRequest
	13, // 6: proto.MonofsManager.ListSnapshots:input_type -> google.protobuf.Empty
	10, // 7: proto.MonofsManager.DeleteSnapshot:input_type -> proto.DeleteSnapshotRequest
	5,  // 8: proto.MonofsManager.GetSnapshot:input_type -> proto.GetSnapshotRequest
	1,  // 9: proto.MonofsStat.Stat:output_type -> proto.StatResponse
	4,  // 10: proto.MonofsProxy.List:output_type -> proto.ListResponse
	8,  // 11: proto.MonofsManager.CreateSnapshot:output_type -> proto.CreateSnapshotResponse
	9,  // 12: proto.MonofsManager.ListSnapshots:output_type -> proto.ListSnapshotsResponse
	11, // 13: proto.MonofsManager.DeleteSnapshot:output_type -> proto.DeleteSnapshotResponse
	6,  // 14: proto.MonofsManager.GetSnapshot:output_type -> proto.GetSnapshotResponse
	9,  // [9:15] is the sub-list for method output_type
	3,  // [3:9] is the sub-list for method input_type
	3,  // [3:3] is the sub-list for extension type_name
	3,  // [3:3] is the sub-list for extension extendee
	0,  // [0:3] is the sub-list for field type_name
}

func init() { file_proto_monoserver_proto_init() }
func file_proto_monoserver_proto_init() {
	if File_proto_monoserver_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_proto_monoserver_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StatRequest); i {
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
		file_proto_monoserver_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StatResponse); i {
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
		file_proto_monoserver_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*File); i {
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
		file_proto_monoserver_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListRequest); i {
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
		file_proto_monoserver_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListResponse); i {
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
		file_proto_monoserver_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetSnapshotRequest); i {
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
		file_proto_monoserver_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetSnapshotResponse); i {
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
		file_proto_monoserver_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CreateSnapshotRequest); i {
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
		file_proto_monoserver_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CreateSnapshotResponse); i {
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
		file_proto_monoserver_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListSnapshotsResponse); i {
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
		file_proto_monoserver_proto_msgTypes[10].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteSnapshotRequest); i {
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
		file_proto_monoserver_proto_msgTypes[11].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteSnapshotResponse); i {
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
			RawDescriptor: file_proto_monoserver_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   12,
			NumExtensions: 0,
			NumServices:   3,
		},
		GoTypes:           file_proto_monoserver_proto_goTypes,
		DependencyIndexes: file_proto_monoserver_proto_depIdxs,
		MessageInfos:      file_proto_monoserver_proto_msgTypes,
	}.Build()
	File_proto_monoserver_proto = out.File
	file_proto_monoserver_proto_rawDesc = nil
	file_proto_monoserver_proto_goTypes = nil
	file_proto_monoserver_proto_depIdxs = nil
}
