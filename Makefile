build:
	go build -o spqr main.go

gogen:
	protoc --go_out=./ --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative \
	protos/shard.proto

init:
	 go mod download
	 go mod vendor

yaccgen:
	goyacc -o yacc/spqrparser/sql.go -p yy yacc/spqrparser/sql.y
