# SPQR Feature tests

## Run feature tests on linux dev enviroment
1. Runing all tests
```bash
GODOG_FEATURE_DIR=generatedFeatures make feature_test; 
```
2. Runing the specified list of tests
Here we run `reference_relation.feature` and `spqrdump.feature` tests:
```bash
GODOG_FEATURE_DIR=generatedFeatures GODOG_FEATURE="reference_relation.feature;spqrdump.feature" make feature_test; 
```

## Troubleshooting

In case you are using Docker or Docker Desktop, everything should work fine. But if you are using Colima, you need to set up something before running the feature test.

> Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?

```bash
export DOCKER_HOST=unix:///Users/denchick/.colima/default/docker.sock
```

> Error response from daemon: client version 1.51 is too new. Maximum supported API version is 1.47, failed to setup compose cluster: Error response from daemon: client version 1.51 is too new. Maximum supported API version is 1.47

```bash
export DOCKER_API_VERSION=1.47
```

In case you are using Rancher Desktop 
> Error: failed to setup compose cluster: Cannot connect to the Docker daemon at unix:///var/run/docker. sock. Is the docker daemon running?

You need enable setting "Allow to acquire administrative credentials (sudo access)" in Preferences -> Application -> General


## Debug in the enviroment similar enviroment of feature tests on linux using VS Code
1. Copy configurations from launch-example.json into .vscode\launch.json configurations for attach router, router2, coordinator and coordinator2.
The following configurations will be available to you:
- Attach router
- Attach router2
- Attach coordinator
- Attach coordinator2
2. Generate images using 
```shell
make build_images
```
It generates image `spqr-base-image-debug` with delve in image.
3. run test enviroment
```shell
docker compose --verbose -f ./test/feature/docker-compose-debug.yaml up
```
4. Attach to the required spqr cluster components in the required order. Only the components you will attach to will work.

### Troubleshooting
- Fix toolchain version in .vscode\launch.json if you have problem in "Step into" action.