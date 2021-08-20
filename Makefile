compile:
	go build src/main/spqr.go && mv spqr spqrpg

gogen:
	protoc --go_out=./genproto --go_opt=paths=source_relative --go-grpc_out=./genproto --go-grpc_opt=paths=source_relative \
	protos/shard.proto

yaccgen:
	goyacc -o spqrparser/sql.go -p yy spqrparser/sql.y
