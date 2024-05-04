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
	rm *.prof
	rm -f ~/.gostore/filters/*
	rm -f ~/.gostore/*.log
	rm -f ~/.gostore/l0/*
	rm -f ~/.gostore/l1/*
	rm -f ~/.gostore/l2/*
	rm -f ~/.gostore/l3/*

test: 
	go test -v -race -memprofile=mem.prof -cpuprofile=cpu.prof -coverprofile=cover.prof ./internal/lsm_tree/

cpu-profile:
	go tool pprof -http=localhost:3001 cpu.prof

mem-profile:
	go tool pprof -http=localhost:3001 mem.prof

cover-profile:
	go tool cover -html=cover.prof

benchmark:
	go test -v -bench=. -benchmem -run=^# ./...

integration-test: clean
	go run cmd/testWrite/main.go && go run cmd/testReplay/main.go
	

.PHONY: run build clean test integration-test proto-compile benchmark cpu-profile mem-profile cover-profile
