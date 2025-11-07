# regress tests

Now CI includes only regress test "without coordinator instalation"  
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
### without coordinator instalation
run for regress without coordinator instalation
```
docker compose --verbose -f ./test/regress/docker-compose.yaml up 2>&1 | grep --line-buffered "regress_tests"
```
and wait for "regress_tests exited with code". this is the end of test.

### with coordinator instalation
run for regress with coordinator 
```
docker compose --verbose -f ./test/regress/docker-compose-coord.yaml up 2>&1 | grep --line-buffered "regress_tests_coord"
```
and wait for "regress_tests_etcd exited with code". this is the end of test.



