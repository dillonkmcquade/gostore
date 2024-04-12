package tests

import (
	"context"
	"testing"
	"time"

	"github.com/dillonkmcquade/gostore/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func getGRPCClient(t *testing.T) (*grpc.ClientConn, pb.GoStoreClient) {
	conn, err := grpc.Dial("127.0.0.1:5000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Errorf("Error on net.Dial: %s", err)
	}
	c := pb.NewGoStoreClient(conn)
	return conn, c
}

func TestNewConn(t *testing.T) {
	conn, c := getGRPCClient(t)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := c.Read(ctx, &pb.ReadRequest{Key: "dill"})
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
	_, err := client.Read(ctx, &pb.ReadRequest{Key: "notfound"})
	if s, ok := status.FromError(err); ok {
		if s.Code() != codes.NotFound {
			t.Error("Code should be not found")
		}
	}
}

func TestType(t *testing.T) {
	conn, client := getGRPCClient(t)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Write(ctx, &pb.WriteRequest{Key: "somekey", Payload: []byte("delete"), Type: pb.Type_STRING.String(), Expiration: 5})
	if err != nil {
		t.Error(err)
	}
	response, err := client.Read(ctx, &pb.ReadRequest{Key: "somekey"})
	if err != nil {
		t.Error("Should return error")
	}
	if response.Type != pb.Type_STRING.String() {
		t.Error("Type should be STRING")
	}
}

func TestDelete(t *testing.T) {
	conn, client := getGRPCClient(t)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Write(ctx, &pb.WriteRequest{Key: "delete", Payload: []byte("delete"), Type: pb.Type_STRING.String(), Expiration: 5})
	if err != nil {
		t.Error(err)
	}
	_, err = client.Delete(ctx, &pb.ReadRequest{Key: "delete"})
	if err != nil {
		t.Error(err)
	}
	_, err = client.Read(ctx, &pb.ReadRequest{Key: "delete"})
	if err == nil {
		t.Error("Should return error")
	}
}

func TestClean(t *testing.T) {
	conn, client := getGRPCClient(t)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := client.Write(ctx, &pb.WriteRequest{Key: "timed", Payload: []byte{1}, Type: pb.Type_BOOL.String(), Expiration: 5})
	if err != nil {
		t.Error(err)
	}
	time.Sleep(6 * time.Second)
	_, err = client.Read(ctx, &pb.ReadRequest{Key: "timed"})
	if err == nil {
		t.Error("Lifetime should have expired")
	}
}
