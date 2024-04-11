package tests

import (
	"context"
	"testing"
	"time"

	gostorepb "github.com/dillonkmcquade/gostore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func getGRPCClient(t *testing.T) (*grpc.ClientConn, gostorepb.GoStoreClient) {
	conn, err := grpc.Dial("127.0.0.1:5000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Errorf("Error on net.Dial: %s", err)
	}
	c := gostorepb.NewGoStoreClient(conn)
	return conn, c
}

func TestNewConn(t *testing.T) {
	conn, c := getGRPCClient(t)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := c.Read(ctx, &gostorepb.ReadRequest{Key: "dill"})
	if err != nil {
		if _, ok := status.FromError(err); !ok {
			t.Error("Should respond with grpc error")
		}
	}
}

func TestError(t *testing.T) {
	conn, client := getGRPCClient(t)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := client.Read(ctx, &gostorepb.ReadRequest{Key: "notfound"})
	if s, ok := status.FromError(err); ok {
		if s.Code() != codes.NotFound {
			t.Error("Code should be not found")
		}
	}
}
