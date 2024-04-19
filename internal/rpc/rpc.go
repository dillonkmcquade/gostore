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
	tree lsm.LSMTree[uint64, []byte]
}

func New() *GoStoreRPC {
	return &GoStoreRPC{tree: lsm.New[uint64, []byte](20)}
}

func (self *GoStoreRPC) Close() {
	self.tree.Close()
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

type ReadResult struct {
	Err error
	Val []byte
}

func (self *GoStoreRPC) Read(ctx context.Context, in *pb.ReadRequest) (*pb.ReadReply, error) {
	done := make(chan *ReadResult, 1)

	go func() {
		val, err := self.tree.Read(in.Key)
		done <- &ReadResult{Err: err, Val: val}
	}()

	select {
	case result := <-done:
		if result.Err != nil {
			return nil, status.Errorf(codes.NotFound, "Not found: %v", result.Err)
		}
		return &pb.ReadReply{Status: int32(codes.OK), Message: "", Data: result.Val}, nil
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
