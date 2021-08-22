compile:
	go build src/main/spqr.go

gogen:
	protoc --go_out=./ --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative \
	protos/shard.proto

init:
	 go mod download
	 go mod vendor

yaccgen:
	goyacc -o src/yacc/spqrparser/sql.go -p yy src/yacc/spqrparser/sql.y
