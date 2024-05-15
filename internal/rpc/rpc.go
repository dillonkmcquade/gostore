package rpc

import (
	"context"

	lsm "github.com/dillonkmcquade/gostore/internal/lsm"
	"github.com/dillonkmcquade/gostore/internal/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GoStoreRPC struct {
	pb.UnimplementedGoStoreServer
	tree lsm.LSM
}

func New() *GoStoreRPC {
	opts := lsm.NewDefaultLSMOpts("")
	tree, err := lsm.New(opts)
	if err != nil {
		panic(err)
	}
	return &GoStoreRPC{tree: tree}
}

func (r *GoStoreRPC) Close() error {
	return r.tree.Close()
}

func (r *GoStoreRPC) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	done := make(chan error, 1)

	go func() { done <- r.tree.Write(in.Key, in.Payload) }()

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

func (r *GoStoreRPC) Read(ctx context.Context, in *pb.ReadRequest) (*pb.ReadReply, error) {
	done := make(chan *ReadResult, 1)

	go func() {
		val, err := r.tree.Read(in.Key)
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

func (r *GoStoreRPC) Delete(ctx context.Context, in *pb.ReadRequest) (*pb.WriteReply, error) {
	done := make(chan error, 1)
	go func() { done <- r.tree.Delete(in.Key) }()
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

func (r *GoStoreRPC) Update(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	done := make(chan error, 1)
	go func() { done <- r.tree.Write(in.Key, in.Payload) }()

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
