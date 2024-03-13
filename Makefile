GIT_REVISION=`git rev-parse --short HEAD`
SPQR_VERSION=`git describe --tags --abbrev=0`
LDFLAGS=-ldflags "-X github.com/pg-sharding/spqr/pkg.GitRevision=${GIT_REVISION} -X github.com/pg-sharding/spqr/pkg.SpqrVersion=${SPQR_VERSION}"
GCFLAGS=-gcflags=all="-N -l"

.PHONY : run
.DEFAULT_GOAL := deps

#################### DEPENDENCIES ####################
proto-deps:
	go get -u google.golang.org/grpc
	go get -u github.com/golang/protobuf/protoc-gen-go
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

yacc-deps:
	go get -u golang.org/x/tools/cmd/goyacc

deps:
	go mod download
	go mod vendor

####################### BUILD #######################

build_coorctl:
	go build -pgo=auto -o coorctl ./cmd/coordctl

build_coordinator: 
	go build -pgo=auto -o spqr-coordinator ./cmd/coordinator

build_router: 
	go build -pgo=auto -o spqr-router $(LDFLAGS) $(GCFLAGS) ./cmd/router

build_mover:
	go build -pgo=auto -o spqr-mover  ./cmd/mover

build_worldmock:
	go build -pgo=auto -o spqr-worldmock ./cmd/worldmock

build_workloadreplay:
	go build -pgo=auto -o spqr-workloadreplay ./cmd/workloadreplay

build_spqrdump:
	go build -pgo=auto -o spqrdump ./cmd/spqrdump

build: build_coordinator build_coorctl build_router build_mover build_worldmock build_workloadreplay build_spqrdump

build_images:
	docker compose build spqr-base-image
	@if [ "x" != "${POSTGRES_VERSION}x" ]; then\
		echo "building ${POSTGRES_VERSION} version";\
		docker compose build --build-arg POSTGRES_VERSION=${POSTGRES_VERSION} spqr-shard-image;\
	else\
		docker compose build spqr-shard-image;\
	fi

save_shard_image:
	sudo rm -f spqr-shard-image-*
	docker compose build ${IMAGE_SHARD};\
	docker save ${IMAGE_SHARD} | gzip -c > ${CACHE_FILE_SHARD};\

clean:
	rm -f spqr-router spqr-coordinator spqr-mover spqr-worldmock
	make clean_feature_test

######################## RUN ########################

run: build_images
	docker compose up -d --remove-orphans --build router router2 coordinator shard1 shard2 qdb01
	docker compose build client
	docker compose run --entrypoint /bin/bash client

proxy_2sh_run:
	./spqr-router run --config ./examples/2shardproxy.yaml -d --proto-debug

proxy_run:
	./spqr-router run --config ./examples/router.yaml

coordinator_run:
	./spqr-coordinator run --config ./examples/coordinator.yaml

pooler_run:
	./spqr-router run --config ./examples/localrouter.yaml

####################### TESTS #######################

unittest:
	go test -race ./cmd/... ./pkg/... ./router/... ./qdb/... ./coordinator/... ./yacc/console...

regress_local: proxy_2sh_run
	./script/regress_local.sh

regress: build_images
	docker compose -f test/regress/docker-compose.yaml down && docker compose -f test/regress/docker-compose.yaml run --build --remove-orphans regress

hibernate_regress: build_images
	docker compose -f test/drivers/hibernate-regress/docker-compose.yaml up --remove-orphans --force-recreate --exit-code-from regress --build coordinator router shard1 shard2 regress qdb01

jdbc_regress: build_images
	docker compose -f test/drivers/jdbc-regress/docker-compose.yaml up --remove-orphans --force-recreate --exit-code-from regress --build coordinator router shard1 shard2 regress qdb01

gorm_regress: build_images
	docker compose -f test/drivers/gorm-regress/docker-compose.yaml down && docker compose -f test/drivers/gorm-regress/docker-compose.yaml run --remove-orphans --build regress

xproto_regress: build_images
	docker compose -f test/xproto/docker-compose.yaml down && docker compose -f test/xproto/docker-compose.yaml run --remove-orphans --build regress

e2e: build_images
	docker compose up --remove-orphans --exit-code-from client --build router coordinator shard1 shard2 qdb01 client

stress: build_images
	docker compose -f test/stress/docker-compose.yaml up --remove-orphans --exit-code-from stress --build router shard1 shard2 stress

split_feature_test_old:
	docker compose build slicer
	(cd test/feature/features; tar -c .) | docker compose run slicer | (mkdir test/feature/generatedFeatures; cd test/feature/generatedFeatures; tar -x)

split_feature_test:
	mkdir test/feature/generatedFeatures && cp test/feature/features/* test/feature/generatedFeatures

clean_feature_test:
	rm -rf test/feature/generatedFeatures

feature_test_ci:
	@if [ "x" = "${CACHE_FILE_SHARD}x" ]; then\
		echo "Rebuild";\
		docker compose build spqr-shard-image;\
	else\
		docker load -i ${CACHE_FILE_SHARD};\
	fi
	docker compose build spqr-base-image
	go build ./test/feature/...
	mkdir ./test/feature/logs
	(cd test/feature; go test -timeout 150m)

feature_test: build_images
	make split_feature_test
	go build ./test/feature/...
	rm -rf ./test/feature/logs
	mkdir ./test/feature/logs
	(cd test/feature; GODOG_FEATURE_DIR=generatedFeatures go test -timeout 150m)
	make clean_feature_test

lint:
	golangci-lint run --timeout=10m --out-format=colored-line-number --skip-dirs=yacc/console

####################### GENERATE #######################

gogen:
	protoc --go_out=./pkg --go_opt=paths=source_relative --go-grpc_out=./pkg --go-grpc_opt=paths=source_relative \
	protos/* 

mockgen:
	mockgen -source=pkg/datatransfers/data_transfers.go -destination=pkg/mock/pgx/mock_pgxconn_iface.go -package=mock
	mockgen -source=pkg/datatransfers/pgx_tx_iface.go -destination=pkg/mock/pgx/mock_pgx_tx.go -package=mock
	mockgen -source=./pkg/conn/raw.go -destination=./pkg/mock/conn/raw_mock.go -package=mock
	mockgen -source=./router/server/server.go -destination=router/mock/server/mock_server.go -package=mock
	mockgen -source=./pkg/conn/instance.go -destination=pkg/mock/conn/mock_instance.go -package=mock
	mockgen -source=./pkg/shard/shard.go -destination=pkg/mock/shard/mock_shard.go -package=mock
	mockgen -source=./router/client/client.go -destination=./router/mock/client/mock_client.go -package=mock
	mockgen -source=./router/poolmgr/pool_mgr.go -destination=./router/mock/poolmgr/mock_pool_mgr.go -package=mock
	mockgen -source=./router/qrouter/qrouter.go -destination=./router/mock/qrouter/mock_qrouter.go -package=mock

yaccgen:
	make -C ./yacc/console gen

gen: gogen yaccgen mockgen

generate: build_images
	docker build -f docker/generator/Dockerfile -t spqr-generator .
	docker run --name spqr-generator-1 spqr-generator
	docker cp spqr-generator-1:/spqr/pkg/protos/. pkg/protos
	docker cp spqr-generator-1:/spqr/yacc/console/. yacc/console
	docker cp spqr-generator-1:/spqr/pkg/mock/. pkg/mock
	docker cp spqr-generator-1:/spqr/router/mock/. router/mock
	docker container rm spqr-generator-1

version = $(shell git describe --tags --abbrev=0)
package:
	sed -i 's/SPQR_VERSION/$(version)/g' debian/changelog
	dpkg-buildpackage -us -uc

.PHONY: build gen
