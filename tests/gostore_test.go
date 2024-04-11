package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	gostorepb "github.com/dillonkmcquade/gostore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestNewConn(t *testing.T) {
	conn, err := grpc.Dial("127.0.0.1:5000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	if err != nil {
		t.Errorf("Error on net.Dial: %s", err)
	}
	c := gostorepb.NewGoStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.Write(ctx, &gostorepb.WriteRequest{Key: "dillon", Payload: "Mcquade"})
	if err != nil {
		t.Error(err)
	}
	fmt.Println(r.String())
	if r.GetStatus() != true {
		t.Error("should be true")
	}
	if r.GetMessage() != "Success" {
		t.Error("should be 'Success'")
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	v, _ := c.Read(ctx, &gostorepb.ReadRequest{Key: "dillon"})
	fmt.Println(v.Value)
	if v.Value != "Mcquade" {
		t.Error("should be Mcquade")
	}
}
