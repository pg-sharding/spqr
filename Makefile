
GO_ROUTER_DIR=$(shell pwd)

TMP_DIR := $(GO_ROUTER_DIR)/tmp

.PHONY: clean

init:
	mkdir $(TMP_DIR)

fmt:
	ya tool yoimports -w ./

compile:
	go build src/main/spqr.go && mv spqr spqrpg

clean:

build-docker-pkg:
	docker build -f ./docker/spqr/Dockerfile . --tag spqrbuild:1.0 && docker run -e VERSION=$(BUILD_VERSION) -e BUILD_NUMBER=$(BUILD_NUM) spqrbuild:1.0

gogen:
	protoc --go_out=./genproto --go_opt=paths=source_relative --go-grpc_out=./genproto --go-grpc_opt=paths=source_relative \
	protos/shard.proto

yaccgen:
	goyacc -o yacc/spqrparser/sql.go -p yy yacc/spqrparser/sql.y
