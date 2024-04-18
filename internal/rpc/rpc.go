package rpc

import (
	"context"

	lsm "github.com/dillonkmcquade/gostore/internal/lsm_tree"
	"github.com/dillonkmcquade/gostore/internal/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GoStoreRPC struct {
	pb.UnimplementedGoStoreServer
	tree        lsm.LSMTree[int64, any]
	cancelChans map[string]chan bool
}

func New() *GoStoreRPC {
	return &GoStoreRPC{tree: lsm.New[int64, any](), cancelChans: make(map[string]chan bool)}
}

func (self *GoStoreRPC) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	done := make(chan error, 1)
	err := self.tree.Write(in.Key, in.Payload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error writing to database: %v", err)
	}

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

func (self *GoStoreRPC) Read(ctx context.Context, in *pb.ReadRequest) (*pb.ReadReply, error) {
	done := make(chan any, 1)
	val, err := self.tree.Read(in.Key)
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
