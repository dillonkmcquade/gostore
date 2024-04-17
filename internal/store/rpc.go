package store

import (
	"context"

	"github.com/dillonkmcquade/gostore/internal/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GoStoreRPC struct {
	pb.UnimplementedGoStoreServer
	DB *DataStore
}

func New() *GoStoreRPC {
	return &GoStoreRPC{DB: &DataStore{data: make(map[string]RawRecord), cancelChans: make(map[string]chan bool)}}
}

// TODO handle ctx
func (self *GoStoreRPC) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	if self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.AlreadyExists, "Existing key found")
	}

	done := make(chan error, 1)
	go self.DB.write(in, done)

	select {
	case err := <-done:
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return &pb.WriteReply{Status: true, Message: "Success"}, nil
	case <-ctx.Done():
		switch ctx.Err() {
		case context.DeadlineExceeded:
			return nil, status.Error(codes.DeadlineExceeded, "Exceeded time limit")
		case context.Canceled:
			return nil, status.Error(codes.Canceled, "Request Cancelled")
		}
	}
	return nil, nil
}

func (self *GoStoreRPC) Read(ctx context.Context, in *pb.ReadRequest) (*pb.Record, error) {
	if !self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.NotFound, "Key not found")
	}
	done := make(chan any, 1)
	go self.DB.read(in.Key, done)
	select {
	case result := <-done:
		switch r := result.(type) {
		case *pb.Record:
			return r, nil
		case error:
			return nil, status.Errorf(codes.Internal, "Error reading from database: %v", r.Error())
		}
	case <-ctx.Done():
		switch ctx.Err() {
		case context.DeadlineExceeded:
			return nil, status.Error(codes.DeadlineExceeded, "Exceeded time limit")
		case context.Canceled:
			return nil, status.Error(codes.Canceled, "Request Cancelled")
		}
	}
	return nil, nil
}

func (self *GoStoreRPC) Delete(ctx context.Context, in *pb.ReadRequest) (*pb.WriteReply, error) {
	if !self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.NotFound, "Key not found")
	}
	done := make(chan bool, 1)
	go self.DB.delete(in.Key, done)
	select {
	case <-done:
		return &pb.WriteReply{Status: true, Message: "Success"}, nil
	case <-ctx.Done():
		switch ctx.Err() {
		case context.DeadlineExceeded:
			return nil, status.Error(codes.DeadlineExceeded, "Exceeded time limit")
		case context.Canceled:
			return nil, status.Error(codes.Canceled, "Request Cancelled")
		}
	}

	return nil, nil
}

func (self *GoStoreRPC) Update(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	if !self.DB.hasKey(in.Key) {
		return nil, status.Error(codes.NotFound, "Key not found")
	}
	done := make(chan error, 1)
	go self.DB.write(in, done)

	select {
	case err := <-done:
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return &pb.WriteReply{Status: true, Message: "Success"}, nil
	case <-ctx.Done():
		switch ctx.Err() {
		case context.DeadlineExceeded:
			return nil, status.Error(codes.DeadlineExceeded, "Exceeded time limit")
		case context.Canceled:
			return nil, status.Error(codes.Canceled, "Request Cancelled")
		}
	}
	return nil, nil
}
