package interceptors

import (
	"context"
	"log"

	"google.golang.org/grpc"
)

// type UnaryServerInterceptor func(ctx context.Context, req any, info *UnaryServerInfo, handler UnaryHandler) (resp any, err error)

func Logger(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	log.Printf("INFO: [RPC] [server-transport %p] Method: %v", info.Server, info.FullMethod)
	return handler(ctx, req)
}
