build:
	go build -o spqr main.go

gogen:
	protoc --go_out=./genproto --go_opt=paths=source_relative --go-grpc_out=./genproto --go-grpc_opt=paths=source_relative \
	protos/*

init:
	 go mod download
	 go mod vendor

test:
	docker-compose up  --remove-orphans --exit-code-from client --build router shard1 shard2 client

yaccgen:
	goyacc -o yacc/spqrparser/sql.go -p yy yacc/spqrparser/sql.y
