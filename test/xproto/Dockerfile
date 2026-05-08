FROM golang:latest

WORKDIR /root
COPY . /root

ENTRYPOINT ["sh", "-c", "go test -tags=${TEST_TAG:-all} -v ./test/xproto/..."]
