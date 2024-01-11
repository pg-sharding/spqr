FROM spqr-base-image

RUN apt-get update && apt-get install -y ragel && apt-get install -y protobuf-compiler
RUN cd $HOME
RUN wget https://go.dev/dl/go1.21.6.linux-amd64.tar.gz
RUN tar -xvf go1.21.6.linux-amd64.tar.gz
RUN export PATH=$GOPATH/bin:$GOROOT/bin:$PATH
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.32.0
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
RUN go install github.com/golang/mock/mockgen@v1.6.0
RUN go install golang.org/x/tools/cmd/goyacc@v0.16.1

ENTRYPOINT cd /spqr && make && make gen