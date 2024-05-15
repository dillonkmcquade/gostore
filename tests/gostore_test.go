package tests

import (
// "context"
// "reflect"
// "testing"
// "time"
//
// "github.com/dillonkmcquade/gostore/internal/pb"
// "google.golang.org/grpc"
// "google.golang.org/grpc/codes"
// "google.golang.org/grpc/credentials/insecure"
// "google.golang.org/grpc/status"
)

// func getGRPCClient(t *testing.T) (*grpc.ClientConn, pb.GoStoreClient) {
// 	conn, err := grpc.Dial("127.0.0.1:5000", grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	if err != nil {
// 		t.Errorf("Error on net.Dial: %s", err)
// 	}
//
// 	c := pb.NewGoStoreClient(conn)
// 	return conn, c
// }
//
// func TestNewRPCConn(t *testing.T) {
// 	conn, c := getGRPCClient(t)
// 	defer conn.Close()
//
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 	defer cancel()
//
// 	_, err := c.Read(ctx, &pb.ReadRequest{Key: 0})
// 	if err != nil {
// 		if _, ok := status.FromError(err); !ok {
// 			t.Error("Should respond with grpc error")
// 		}
// 	}
// }
//
// func TestRPCError(t *testing.T) {
// 	conn, client := getGRPCClient(t)
// 	defer conn.Close()
//
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 	defer cancel()
// 	_, err := client.Read(ctx, &pb.ReadRequest{Key: 1000})
// 	if s, ok := status.FromError(err); ok {
// 		if s.Code() != codes.NotFound {
// 			t.Error("Code should be not found")
// 		}
// 	}
// }
//
// func TestRPCRead(t *testing.T) {
// 	conn, client := getGRPCClient(t)
// 	defer conn.Close()
//
// 	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
// 	defer cancel()
//
// 	_, err := client.Write(ctx, &pb.WriteRequest{Key: 10, Payload: []byte("delete")})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	response, err := client.Read(ctx, &pb.ReadRequest{Key: 10})
// 	if err != nil {
// 		t.Error("Error on read")
// 	}
// 	if !reflect.DeepEqual(response.GetData(), []byte("delete")) {
// 		t.Errorf("%v equal 'delete'", response.GetData())
// 	}
// }
//
// func TestRPCDelete(t *testing.T) {
// 	conn, client := getGRPCClient(t)
// 	defer conn.Close()
//
// 	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
// 	defer cancel()
//
// 	_, err := client.Write(ctx, &pb.WriteRequest{Key: 5, Payload: []byte("delete")})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	_, err = client.Delete(ctx, &pb.ReadRequest{Key: 5})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	_, err = client.Read(ctx, &pb.ReadRequest{Key: 5})
// 	if err == nil {
// 		t.Error("Should return error")
// 	}
// }
