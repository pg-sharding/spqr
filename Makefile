
deps:
	go get golang.org/x/tools/cmd/goyacc
	go get -u github.com/golang/protobuf/protoc-gen-go
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc


build: 
	go build -o spqr-pg main.go

gen: gogen yaccgen

gogen:
	protoc --go_out=./genproto --go_opt=paths=source_relative --go-grpc_out=./genproto --go-grpc_opt=paths=source_relative \
	protos/spqr/*

init:
	 go mod download
	 go mod vendor

test:
	docker-compose up  --remove-orphans --exit-code-from client --build router shard1 shard2 client

run:
	docker-compose up -d --remove-orphans --build router shard1 shard2
	docker run -it --entrypoint /bin/bash spqr_client


yaccgen:
	goyacc -o yacc/spqrparser/sql.go -p yy yacc/spqrparser/sql.y

.PHONY: build gen
