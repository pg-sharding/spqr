FROM golang:latest

WORKDIR /root
COPY . /root

ENTRYPOINT ["sh", "-c", "timeout 10m go test -tags=${TEST_TAG:-all} -v -bench '^BenchmarkDistributedUpsert$' -benchtime=10000x -count=1 ./test/xproto/..."]
