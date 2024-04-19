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
	tree lsm.LSMTree[int64, []byte]
}

func New() *GoStoreRPC {
	return &GoStoreRPC{tree: lsm.New[int64, []byte](20)}
}

func (self *GoStoreRPC) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	done := make(chan error, 1)

	go func() { done <- self.tree.Write(in.Key, in.Payload) }()

	select {
	case err := <-done:
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return &pb.WriteReply{Status: int32(codes.OK), Message: "Success"}, nil
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

	go func() {
		val, err := self.tree.Read(in.Key)
		if err != nil {
			done <- err
			return
		}
		done <- val
	}()

	select {
	case result := <-done:
		switch r := result.(type) {
		case error:
			return nil, status.Errorf(codes.Internal, "Error reading from database: %v", r.Error())
		case []byte:
			return &pb.ReadReply{Status: int32(codes.OK), Message: "", Data: r}, nil
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
	done := make(chan error, 1)
	go func() { done <- self.tree.Delete(in.Key) }()
	select {
	case err := <-done:
		return &pb.WriteReply{Status: int32(codes.OK), Message: err.Error()}, nil
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
	done := make(chan error, 1)
	go func() { done <- self.tree.Write(in.Key, in.Payload) }()

	select {
	case err := <-done:
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return &pb.WriteReply{Status: int32(codes.OK), Message: "Success"}, nil
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
