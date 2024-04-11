package store

import (
	"context"

	"github.com/dillonkmcquade/gostore/internal/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GoStore struct {
	pb.UnimplementedGoStoreServer
	DB *DataStore
}

func New() *GoStore {
	return &GoStore{DB: &DataStore{data: make(map[string]string)}}
}

func (self *GoStore) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	if self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.AlreadyExists, "Existing key found")
	}
	self.DB.write(in.Key, in.Payload)
	return &pb.WriteReply{Status: true, Message: "Success"}, nil
}

func (self *GoStore) Read(ctx context.Context, in *pb.ReadRequest) (*pb.ReadReply, error) {
	if !self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.NotFound, "Key not found")
	}
	v := self.DB.read(in.Key)
	return &pb.ReadReply{Value: v}, nil
}

func (self *GoStore) Delete(ctx context.Context, in *pb.ReadRequest) (*pb.WriteReply, error) {
	if !self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.NotFound, "Key not found")
	}
	return &pb.WriteReply{Status: true, Message: "Success"}, nil
}

func (self *GoStore) Update(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	if !self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.NotFound, "Key not found")
	}
	self.DB.write(in.Key, in.Payload)
	return &pb.WriteReply{Status: true, Message: "Success"}, nil
}
