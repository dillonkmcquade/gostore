package store

import (
	"context"
	"time"

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

// Removes the record from the store after the specified time
func (self *GoStore) Clean(key string, t time.Duration) {
	time.Sleep(t)
	self.DB.delete(key)
}

func (self *GoStore) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	if self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.AlreadyExists, "Existing key found")
	}
	err := self.DB.write(in.Key, in.Payload, in.Type)

	// TODO: Update Write declaraction in .proto file to accept a time in minutes
	go self.Clean(in.Key, 5*time.Second)

	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.WriteReply{Status: true, Message: "Success"}, nil
}

func (self *GoStore) Read(ctx context.Context, in *pb.ReadRequest) (*pb.Record, error) {
	if !self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.NotFound, "Key not found")
	}
	r, err := self.DB.read(in.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "An error occurred while reading from database: %v", err)
	}
	return r, nil
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
