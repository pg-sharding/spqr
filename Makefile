.PHONY : run
.DEFAULT_GOAL := deps

proto-deps:
	go get -u google.golang.org/grpc
	go get -u github.com/golang/protobuf/protoc-gen-go
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

yacc-deps:
	go get -u golang.org/x/tools/cmd/goyacc

deps:
	go mod download

build_coordinator: 
	go build -o spqr-coordinator cmd/coordinator/main.go

build_router: 
	go build -o spqr-router cmd/router/main.go

build_world: 
	go build -o spqr-world cmd/world/main.go

build_stress:
	go build -o spqr-stress test/stress/stress.go

build_worldmock:
	go build -o spqr-worldmock ./cmd/worldmock/main.go

build_balancer:
	go build -o spqr-balancer ./cmd/balancer/main.go

build_mover:
	go build -o spqr-mover  ./cmd/mover/main.go

build: build_balancer build_coordinator build_router build_stress  build_mover build_world build_worldmock

gogen:
	protoc --go_out=./router --go_opt=paths=source_relative --go-grpc_out=./router --go-grpc_opt=paths=source_relative \
	protos/* 

yaccgen:
	goyacc -o yacc/console/sql.go -p yy yacc/console/sql.y

gen: gogen yaccgen

init:
	 go mod download
	 go mod vendor

build_images:
	docker-compose build spqrbase shardbase

test: build_images
	docker-compose up --remove-orphans --exit-code-from client --build router coordinator shard1 shard2 qdb01 client 

stress: build_images
	docker-compose up -d --remove-orphans --build router coordinator shard1 shard2 qdb01
	docker-compose build client
	docker-compose run --entrypoint /usr/local/bin/stress_test.sh client

run: build_images
	docker-compose up -d --remove-orphans --build router coordinator shard1 shard2 qdb01
	docker-compose build client
	docker-compose run --entrypoint /bin/bash client

proxy_run:
	./spqr-router run -c ./config-example/router.yaml

coordinator_run:
	./spqr-coordinator run -c ./config-example/coordinator.yaml

pooler_run:
	./spqr-router run -c ./config-example/localrouter.yaml

clean:
	rm -f spqr-router spqr-coordinator spqr-mover spqr-stress spqr-worldmock spqr-world spqr-balancer

make regress: build_images
	docker-compose -f test/regress/docker-compose.yaml up --remove-orphans --exit-code-from regress --build coordinator shard1 shard2 regress

.PHONY: build gen
