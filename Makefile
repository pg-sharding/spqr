compile:
	go build src/main/spqr.go && mv spqr spqrpg

gogen:
	protoc --go_out=./genproto --go_opt=paths=source_relative --go-grpc_out=./genproto --go-grpc_opt=paths=source_relative \
	protos/shard.proto

yaccgen:
	goyacc -o yacc/spqrparser/sql.go -p yy yacc/spqrparser/sql.y
