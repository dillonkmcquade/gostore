package main

import (
	"flag"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/dillonkmcquade/gostore/internal/interceptors"
	"github.com/dillonkmcquade/gostore/internal/pb"
	"github.com/dillonkmcquade/gostore/internal/rpc"
	"google.golang.org/grpc"
)

var (
	port       = flag.Int("port", 5000, "The port to listen on")
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile = flag.String("memprofile", "", "write memory profile to `file`")
)

func main() {
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

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
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(interceptors.Logger))
	defer s.Stop()

	srv := rpc.New()
	defer srv.Close()
	pb.RegisterGoStoreServer(s, srv)

	if err = s.Serve(listener); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}
