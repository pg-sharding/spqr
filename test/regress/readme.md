# regress tests

Now CI includes only regress test "without coordinator installation"  
## How run regress tests locally:
for linux dev environment
### Prepare runing
Stop, remove old containers and images. Rebuild images.
```
docker ps -a | grep -P 'spqr|feature|regress' |  awk '{print $1}' | xargs docker stop;
docker ps -a | grep -P 'spqr|feature|regress' |  awk '{print $1}' | xargs docker rm;
docker images | grep -P 'spqr|feature|regress' | awk '{print $3}' | xargs  docker rmi;
make build_images;
```
### without coordinator installation
run for regress without coordinator installation
```
docker compose --verbose -f ./test/regress/docker-compose.yaml up 2>&1 | grep --line-buffered "regress_tests"
```
and wait for "regress_tests exited with code". this is the end of test.

### with coordinator installation
run for regress with coordinator 
```
docker compose --verbose -f ./test/regress/docker-compose-coord.yaml up 2>&1 | grep --line-buffered "regress_tests_coord"
```
and wait for "regress_tests_etcd exited with code". this is the end of test.

### through an odyssey pooler

Same topology as "without coordinator installation", but every shard is fronted
by an [odyssey](https://github.com/yandex/odyssey) pooler, so the path is
`pg_regress -> router -> odyssey -> postgres`. The odyssey containers take the
`spqr_shard_*` names and postgres moves to `spqr_shard_*_pg`, which lets the run
reuse `conf/router.yaml` and the regular expected outputs.

```
make regress_odyssey
```
or
```
docker compose --verbose -f ./test/regress/docker-compose-odyssey.yaml up 2>&1 | grep --line-buffered "regress_tests_odyssey"
```

The suite asserts the same expected outputs as `make regress`; the router
schedule runs as is except for the tests listed in `schedule/odyssey.skip`. The
pooler route lives in `conf/odyssey.conf`, and `ODYSSEY_IMAGE` points the run at
another odyssey build.



