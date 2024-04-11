package main

import (
	"flag"
	"log"
	"net"

	"github.com/dillonkmcquade/gostore/internal/pb"
	"github.com/dillonkmcquade/gostore/internal/store"
	"google.golang.org/grpc"
)

var port = flag.Int("port", 5000, "The port to listen on")

func main() {
	flag.Parse()

	if *port < 1024 {
		log.Fatalf("Port %d is restricted to root user only, try using another port", port)
	}

	addr := &net.TCPAddr{
		IP:   []byte{127, 0, 0, 1},
		Port: *port,
	}

	listener, err := net.ListenTCP("tcp", addr)
	defer listener.Close()
	if err != nil {
		panic(err)
	}

	s := grpc.NewServer()
	defer s.Stop()

	pb.RegisterGoStoreServer(s, store.New())

	if err = s.Serve(listener); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}
