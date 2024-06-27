// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.20.3
// source: src/proto/core.proto

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

type WorkerState int32

const (
	WorkerState_NULL    WorkerState = 0
	WorkerState_IDLE    WorkerState = 1
	WorkerState_WORKING WorkerState = 2
)

// Enum value maps for WorkerState.
var (
	WorkerState_name = map[int32]string{
		0: "NULL",
		1: "IDLE",
		2: "WORKING",
	}
	WorkerState_value = map[string]int32{
		"NULL":    0,
		"IDLE":    1,
		"WORKING": 2,
	}
)

func (x WorkerState) Enum() *WorkerState {
	p := new(WorkerState)
	*p = x
	return p
}

func (x WorkerState) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (WorkerState) Descriptor() protoreflect.EnumDescriptor {
	return file_src_proto_core_proto_enumTypes[0].Descriptor()
}

func (WorkerState) Type() protoreflect.EnumType {
	return &file_src_proto_core_proto_enumTypes[0]
}

func (x WorkerState) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use WorkerState.Descriptor instead.
func (WorkerState) EnumDescriptor() ([]byte, []int) {
	return file_src_proto_core_proto_rawDescGZIP(), []int{0}
}

type HeartbeatRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	WorkerAddr string      `protobuf:"bytes,2,opt,name=workerAddr,proto3" json:"workerAddr,omitempty"`
	State      WorkerState `protobuf:"varint,3,opt,name=state,proto3,enum=core.WorkerState" json:"state,omitempty"`
}

func (x *HeartbeatRequest) Reset() {
	*x = HeartbeatRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_src_proto_core_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HeartbeatRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HeartbeatRequest) ProtoMessage() {}

func (x *HeartbeatRequest) ProtoReflect() protoreflect.Message {
	mi := &file_src_proto_core_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HeartbeatRequest.ProtoReflect.Descriptor instead.
func (*HeartbeatRequest) Descriptor() ([]byte, []int) {
	return file_src_proto_core_proto_rawDescGZIP(), []int{0}
}

func (x *HeartbeatRequest) GetWorkerAddr() string {
	if x != nil {
		return x.WorkerAddr
	}
	return ""
}

func (x *HeartbeatRequest) GetState() WorkerState {
	if x != nil {
		return x.State
	}
	return WorkerState_NULL
}

type HeartbeatReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *HeartbeatReply) Reset() {
	*x = HeartbeatReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_src_proto_core_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HeartbeatReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HeartbeatReply) ProtoMessage() {}

func (x *HeartbeatReply) ProtoReflect() protoreflect.Message {
	mi := &file_src_proto_core_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HeartbeatReply.ProtoReflect.Descriptor instead.
func (*HeartbeatReply) Descriptor() ([]byte, []int) {
	return file_src_proto_core_proto_rawDescGZIP(), []int{1}
}

type File struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Path      string `protobuf:"bytes,1,opt,name=path,proto3" json:"path,omitempty"`
	SizeBytes uint64 `protobuf:"varint,2,opt,name=sizeBytes,proto3" json:"sizeBytes,omitempty"`
}

func (x *File) Reset() {
	*x = File{}
	if protoimpl.UnsafeEnabled {
		mi := &file_src_proto_core_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *File) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*File) ProtoMessage() {}

func (x *File) ProtoReflect() protoreflect.Message {
	mi := &file_src_proto_core_proto_msgTypes[2]
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
	return file_src_proto_core_proto_rawDescGZIP(), []int{2}
}

func (x *File) GetPath() string {
	if x != nil {
		return x.Path
	}
	return ""
}

func (x *File) GetSizeBytes() uint64 {
	if x != nil {
		return x.SizeBytes
	}
	return 0
}

type Task struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskID      uint64  `protobuf:"varint,1,opt,name=taskID,proto3" json:"taskID,omitempty"`
	OutputFiles []*File `protobuf:"bytes,2,rep,name=outputFiles,proto3" json:"outputFiles,omitempty"`
}

func (x *Task) Reset() {
	*x = Task{}
	if protoimpl.UnsafeEnabled {
		mi := &file_src_proto_core_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Task) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Task) ProtoMessage() {}

func (x *Task) ProtoReflect() protoreflect.Message {
	mi := &file_src_proto_core_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Task.ProtoReflect.Descriptor instead.
func (*Task) Descriptor() ([]byte, []int) {
	return file_src_proto_core_proto_rawDescGZIP(), []int{3}
}

func (x *Task) GetTaskID() uint64 {
	if x != nil {
		return x.TaskID
	}
	return 0
}

func (x *Task) GetOutputFiles() []*File {
	if x != nil {
		return x.OutputFiles
	}
	return nil
}

type MapTasksCompletedRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	WorkerAddr string  `protobuf:"bytes,1,opt,name=workerAddr,proto3" json:"workerAddr,omitempty"`
	Tasks      []*Task `protobuf:"bytes,2,rep,name=tasks,proto3" json:"tasks,omitempty"`
}

func (x *MapTasksCompletedRequest) Reset() {
	*x = MapTasksCompletedRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_src_proto_core_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MapTasksCompletedRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MapTasksCompletedRequest) ProtoMessage() {}

func (x *MapTasksCompletedRequest) ProtoReflect() protoreflect.Message {
	mi := &file_src_proto_core_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MapTasksCompletedRequest.ProtoReflect.Descriptor instead.
func (*MapTasksCompletedRequest) Descriptor() ([]byte, []int) {
	return file_src_proto_core_proto_rawDescGZIP(), []int{4}
}

func (x *MapTasksCompletedRequest) GetWorkerAddr() string {
	if x != nil {
		return x.WorkerAddr
	}
	return ""
}

func (x *MapTasksCompletedRequest) GetTasks() []*Task {
	if x != nil {
		return x.Tasks
	}
	return nil
}

type MapTasksCompletedReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *MapTasksCompletedReply) Reset() {
	*x = MapTasksCompletedReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_src_proto_core_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MapTasksCompletedReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MapTasksCompletedReply) ProtoMessage() {}

func (x *MapTasksCompletedReply) ProtoReflect() protoreflect.Message {
	mi := &file_src_proto_core_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MapTasksCompletedReply.ProtoReflect.Descriptor instead.
func (*MapTasksCompletedReply) Descriptor() ([]byte, []int) {
	return file_src_proto_core_proto_rawDescGZIP(), []int{5}
}

type AssignMapTaskRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskID   uint64 `protobuf:"varint,1,opt,name=taskID,proto3" json:"taskID,omitempty"`
	FileID   uint64 `protobuf:"varint,2,opt,name=fileID,proto3" json:"fileID,omitempty"`
	FilePath string `protobuf:"bytes,3,opt,name=filePath,proto3" json:"filePath,omitempty"`
	Script   string `protobuf:"bytes,4,opt,name=script,proto3" json:"script,omitempty"`
}

func (x *AssignMapTaskRequest) Reset() {
	*x = AssignMapTaskRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_src_proto_core_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AssignMapTaskRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AssignMapTaskRequest) ProtoMessage() {}

func (x *AssignMapTaskRequest) ProtoReflect() protoreflect.Message {
	mi := &file_src_proto_core_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AssignMapTaskRequest.ProtoReflect.Descriptor instead.
func (*AssignMapTaskRequest) Descriptor() ([]byte, []int) {
	return file_src_proto_core_proto_rawDescGZIP(), []int{6}
}

func (x *AssignMapTaskRequest) GetTaskID() uint64 {
	if x != nil {
		return x.TaskID
	}
	return 0
}

func (x *AssignMapTaskRequest) GetFileID() uint64 {
	if x != nil {
		return x.FileID
	}
	return 0
}

func (x *AssignMapTaskRequest) GetFilePath() string {
	if x != nil {
		return x.FilePath
	}
	return ""
}

func (x *AssignMapTaskRequest) GetScript() string {
	if x != nil {
		return x.Script
	}
	return ""
}

type AssignMapTaskRequestReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *AssignMapTaskRequestReply) Reset() {
	*x = AssignMapTaskRequestReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_src_proto_core_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AssignMapTaskRequestReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AssignMapTaskRequestReply) ProtoMessage() {}

func (x *AssignMapTaskRequestReply) ProtoReflect() protoreflect.Message {
	mi := &file_src_proto_core_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AssignMapTaskRequestReply.ProtoReflect.Descriptor instead.
func (*AssignMapTaskRequestReply) Descriptor() ([]byte, []int) {
	return file_src_proto_core_proto_rawDescGZIP(), []int{7}
}

var File_src_proto_core_proto protoreflect.FileDescriptor

var file_src_proto_core_proto_rawDesc = []byte{
	0x0a, 0x14, 0x73, 0x72, 0x63, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x72, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04, 0x63, 0x6f, 0x72, 0x65, 0x22, 0x5b, 0x0a, 0x10,
	0x48, 0x65, 0x61, 0x72, 0x74, 0x62, 0x65, 0x61, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x1e, 0x0a, 0x0a, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x41, 0x64, 0x64, 0x72, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x41, 0x64, 0x64, 0x72,
	0x12, 0x27, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32,
	0x11, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x53, 0x74, 0x61,
	0x74, 0x65, 0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x22, 0x10, 0x0a, 0x0e, 0x48, 0x65, 0x61,
	0x72, 0x74, 0x62, 0x65, 0x61, 0x74, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x38, 0x0a, 0x04, 0x46,
	0x69, 0x6c, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x61, 0x74, 0x68, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x70, 0x61, 0x74, 0x68, 0x12, 0x1c, 0x0a, 0x09, 0x73, 0x69, 0x7a, 0x65, 0x42,
	0x79, 0x74, 0x65, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x73, 0x69, 0x7a, 0x65,
	0x42, 0x79, 0x74, 0x65, 0x73, 0x22, 0x4c, 0x0a, 0x04, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x16, 0x0a,
	0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x06, 0x74,
	0x61, 0x73, 0x6b, 0x49, 0x44, 0x12, 0x2c, 0x0a, 0x0b, 0x6f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x46,
	0x69, 0x6c, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0a, 0x2e, 0x63, 0x6f, 0x72,
	0x65, 0x2e, 0x46, 0x69, 0x6c, 0x65, 0x52, 0x0b, 0x6f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x46, 0x69,
	0x6c, 0x65, 0x73, 0x22, 0x5c, 0x0a, 0x18, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x43,
	0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x1e, 0x0a, 0x0a, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x41, 0x64, 0x64, 0x72, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0a, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x41, 0x64, 0x64, 0x72, 0x12,
	0x20, 0x0a, 0x05, 0x74, 0x61, 0x73, 0x6b, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0a,
	0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x05, 0x74, 0x61, 0x73, 0x6b,
	0x73, 0x22, 0x18, 0x0a, 0x16, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x43, 0x6f, 0x6d,
	0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x7a, 0x0a, 0x14, 0x41,
	0x73, 0x73, 0x69, 0x67, 0x6e, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x44, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x04, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x44, 0x12, 0x16, 0x0a, 0x06, 0x66,
	0x69, 0x6c, 0x65, 0x49, 0x44, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x06, 0x66, 0x69, 0x6c,
	0x65, 0x49, 0x44, 0x12, 0x1a, 0x0a, 0x08, 0x66, 0x69, 0x6c, 0x65, 0x50, 0x61, 0x74, 0x68, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x66, 0x69, 0x6c, 0x65, 0x50, 0x61, 0x74, 0x68, 0x12,
	0x16, 0x0a, 0x06, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x06, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x22, 0x1b, 0x0a, 0x19, 0x41, 0x73, 0x73, 0x69, 0x67,
	0x6e, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x52,
	0x65, 0x70, 0x6c, 0x79, 0x2a, 0x2e, 0x0a, 0x0b, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x53, 0x74,
	0x61, 0x74, 0x65, 0x12, 0x08, 0x0a, 0x04, 0x4e, 0x55, 0x4c, 0x4c, 0x10, 0x00, 0x12, 0x08, 0x0a,
	0x04, 0x49, 0x44, 0x4c, 0x45, 0x10, 0x01, 0x12, 0x0b, 0x0a, 0x07, 0x57, 0x4f, 0x52, 0x4b, 0x49,
	0x4e, 0x47, 0x10, 0x02, 0x32, 0x9a, 0x01, 0x0a, 0x06, 0x4d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x12,
	0x3b, 0x0a, 0x09, 0x48, 0x65, 0x61, 0x72, 0x74, 0x62, 0x65, 0x61, 0x74, 0x12, 0x16, 0x2e, 0x63,
	0x6f, 0x72, 0x65, 0x2e, 0x48, 0x65, 0x61, 0x72, 0x74, 0x62, 0x65, 0x61, 0x74, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x14, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x48, 0x65, 0x61, 0x72,
	0x74, 0x62, 0x65, 0x61, 0x74, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x00, 0x12, 0x53, 0x0a, 0x11,
	0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x43, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65,
	0x64, 0x12, 0x1e, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b,
	0x73, 0x43, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x1c, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b,
	0x73, 0x43, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22,
	0x00, 0x32, 0x58, 0x0a, 0x06, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x12, 0x4e, 0x0a, 0x0d, 0x41,
	0x73, 0x73, 0x69, 0x67, 0x6e, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x1a, 0x2e, 0x63,
	0x6f, 0x72, 0x65, 0x2e, 0x41, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73,
	0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1f, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e,
	0x41, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x00, 0x42, 0x37, 0x5a, 0x35, 0x67,
	0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x70, 0x6f, 0x6f, 0x72, 0x6c, 0x79,
	0x64, 0x65, 0x66, 0x69, 0x6e, 0x65, 0x64, 0x62, 0x65, 0x68, 0x61, 0x76, 0x69, 0x6f, 0x75, 0x72,
	0x2f, 0x6d, 0x61, 0x70, 0x5f, 0x72, 0x65, 0x64, 0x75, 0x63, 0x65, 0x5f, 0x67, 0x6f, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_src_proto_core_proto_rawDescOnce sync.Once
	file_src_proto_core_proto_rawDescData = file_src_proto_core_proto_rawDesc
)

func file_src_proto_core_proto_rawDescGZIP() []byte {
	file_src_proto_core_proto_rawDescOnce.Do(func() {
		file_src_proto_core_proto_rawDescData = protoimpl.X.CompressGZIP(file_src_proto_core_proto_rawDescData)
	})
	return file_src_proto_core_proto_rawDescData
}

var file_src_proto_core_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_src_proto_core_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_src_proto_core_proto_goTypes = []interface{}{
	(WorkerState)(0),                  // 0: core.WorkerState
	(*HeartbeatRequest)(nil),          // 1: core.HeartbeatRequest
	(*HeartbeatReply)(nil),            // 2: core.HeartbeatReply
	(*File)(nil),                      // 3: core.File
	(*Task)(nil),                      // 4: core.Task
	(*MapTasksCompletedRequest)(nil),  // 5: core.MapTasksCompletedRequest
	(*MapTasksCompletedReply)(nil),    // 6: core.MapTasksCompletedReply
	(*AssignMapTaskRequest)(nil),      // 7: core.AssignMapTaskRequest
	(*AssignMapTaskRequestReply)(nil), // 8: core.AssignMapTaskRequestReply
}
var file_src_proto_core_proto_depIdxs = []int32{
	0, // 0: core.HeartbeatRequest.state:type_name -> core.WorkerState
	3, // 1: core.Task.outputFiles:type_name -> core.File
	4, // 2: core.MapTasksCompletedRequest.tasks:type_name -> core.Task
	1, // 3: core.Master.Heartbeat:input_type -> core.HeartbeatRequest
	5, // 4: core.Master.MapTasksCompleted:input_type -> core.MapTasksCompletedRequest
	7, // 5: core.Worker.AssignMapTask:input_type -> core.AssignMapTaskRequest
	2, // 6: core.Master.Heartbeat:output_type -> core.HeartbeatReply
	6, // 7: core.Master.MapTasksCompleted:output_type -> core.MapTasksCompletedReply
	8, // 8: core.Worker.AssignMapTask:output_type -> core.AssignMapTaskRequestReply
	6, // [6:9] is the sub-list for method output_type
	3, // [3:6] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_src_proto_core_proto_init() }
func file_src_proto_core_proto_init() {
	if File_src_proto_core_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_src_proto_core_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HeartbeatRequest); i {
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
		file_src_proto_core_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HeartbeatReply); i {
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
		file_src_proto_core_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
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
		file_src_proto_core_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Task); i {
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
		file_src_proto_core_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MapTasksCompletedRequest); i {
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
		file_src_proto_core_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MapTasksCompletedReply); i {
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
		file_src_proto_core_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AssignMapTaskRequest); i {
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
		file_src_proto_core_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AssignMapTaskRequestReply); i {
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
			RawDescriptor: file_src_proto_core_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   2,
		},
		GoTypes:           file_src_proto_core_proto_goTypes,
		DependencyIndexes: file_src_proto_core_proto_depIdxs,
		EnumInfos:         file_src_proto_core_proto_enumTypes,
		MessageInfos:      file_src_proto_core_proto_msgTypes,
	}.Build()
	File_src_proto_core_proto = out.File
	file_src_proto_core_proto_rawDesc = nil
	file_src_proto_core_proto_goTypes = nil
	file_src_proto_core_proto_depIdxs = nil
}
