SHELL = /bin/sh

debug := GRPC_GO_LOG_VERBOSITY_LEVEL=99 GRPC_GO_LOG_SEVERITY_LEVEL=info

build:
	go build -o gostore cmd/gostore 

proto-compile:
	protoc -I=proto --go_out=internal/pb --go_opt=paths=source_relative \
	--go-grpc_out=internal/pb --go-grpc_opt=paths=source_relative \
	proto/gostore.proto

run:
	$(debug) go run cmd/gostore/main.go -port=5000

clean:
	rm gostore

test: 
	go test -v ./...


.PHONY: run build clean test proto-compile
