SHELL = /bin/sh

debug := GRPC_GO_LOG_VERBOSITY_LEVEL=99 GRPC_GO_LOG_SEVERITY_LEVEL=info

build:
	go build -o gostore cmd/gostore/main.go

proto-compile:
	protoc -I=proto --go_out=internal/pb --go_opt=paths=source_relative \
	--go-grpc_out=internal/pb --go-grpc_opt=paths=source_relative \
	proto/gostore.proto

run:
	$(debug) go run cmd/gostore/main.go -port=5000

clean:
	rm --force gostore
	rm -f ~/.gostore/filters/*
	rm -f ~/.gostore/*.log
	rm -f ~/.gostore/l0/*
	rm -f ~/.gostore/l1/*
	rm -f ~/.gostore/l2/*
	rm -f ~/.gostore/l3/*

test: clean 
	go test -v -race ./internal/...

integration-test:
	go test -v -race ./tests/... -count=1


.PHONY: run build clean test integration-test proto-compile
