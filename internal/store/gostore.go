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
	return &GoStore{DB: &DataStore{data: make(map[string]RawRecord)}}
}

func (self *GoStore) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	if self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.AlreadyExists, "Existing key found")
	}
	err := self.DB.write(in.Key, in.Payload, in.Type)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.WriteReply{Status: true, Message: "Success"}, nil
}

func (self *GoStore) Read(ctx context.Context, in *pb.ReadRequest) (*pb.ReadReply, error) {
	if !self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.NotFound, "Key not found")
	}
	r, err := self.DB.read(in.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "An error occurred while reading from database: %v", err)
	}
	return &pb.ReadReply{Payload: r.Payload, Type: r.RecordType}, nil
}

func (self *GoStore) Delete(ctx context.Context, in *pb.ReadRequest) (*pb.WriteReply, error) {
	if !self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.NotFound, "Key not found")
	}
	self.DB.delete(in.Key)
	return &pb.WriteReply{Status: true, Message: "Success"}, nil
}

func (self *GoStore) Update(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	if !self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.NotFound, "Key not found")
	}
	self.DB.write(in.Key, in.Payload, in.Type)
	return &pb.WriteReply{Status: true, Message: "Success"}, nil
}
