
GO_ROUTER_DIR=$(shell pwd)

TMP_DIR := $(GO_ROUTER_DIR)/tmp

.PHONY: clean

init:
	mkdir $(TMP_DIR)

fmt:
	ya tool yoimports -w ./

compile:
	go build src/main/shgo.go -o shgo

clean:
	rm -f ./main

build-docker-pkg:
	docker build -f ./docker/shgo/Dockerfile . --tag shgobuild:1.0 && docker run -e VERSION=$(BUILD_VERSION) -e BUILD_NUMBER=$(BUILD_NUM) shgobuild:1.0

gogen:
	protoc --go_out=./genproto --go_opt=paths=source_relative \
    --go-grpc_out=./genproto --go-grpc_opt=paths=source_relative \
   protos/shard.proto
