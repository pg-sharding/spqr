# SPQR Feature tests

## Run feature tests on linux dev environment
1. Runing all tests
```bash
GODOG_FEATURE_DIR=generatedFeatures make feature_test; 
```
2. Runing the specified list of tests
Here we run `reference_relation.feature` and `spqrdump.feature` tests:
```bash
GODOG_FEATURE_DIR=generatedFeatures GODOG_FEATURE="reference_relation.feature;spqrdump.feature" make feature_test; 
```

## Odyssey compatibility tests

In production SPQR is reached through the [Odyssey](https://github.com/yandex/odyssey)
connection pooler rather than directly, and the router has to keep working when the pooler
is upgraded. `odyssey.feature` covers that path, and it is run against several Odyssey
versions instead of one pinned version.

### Topology

The `odyssey` service in `docker-compose.yaml` puts Odyssey in front of the router:

```text
test client -> odyssey -> router -> shards
```

It only starts when `COMPOSE_PROFILES` contains `odyssey`, so the default feature test
topology is untouched. Every Odyssey scenario also keeps talking to `router` directly, so
a failure through the pooler can be told apart from a failure of SPQR itself.

Odyssey comes from the public `ghcr.io/yandex/odyssey` images and builds its own config
from the environment, which is why no config file is checked in here: the config format is
one of the things that can change between versions, and not depending on it keeps the
scenarios version agnostic. For the same reason the service declares no healthcheck — the
image contents differ across versions (1.5.0 is Alpine and ships `nc`, 1.5.1 is Debian and
does not) — readiness is established by the test harness retrying the connection.

Only `linux/amd64` images are published upstream, so the service pins that platform and
runs under emulation on arm64 development machines.

### Running locally

```bash
make feature_test_odyssey
```

To pick a version:

```bash
make feature_test_odyssey ODYSSEY_IMAGE=ghcr.io/yandex/odyssey:1.5.0
```

Pool mode is chosen per scenario through `ODYSSEY_POOL_TYPE` in `cluster environment is`,
so both session and transaction pooling are covered without a CI matrix dimension.

Because a dedicated job owns it, `odyssey.feature` is excluded from the general feature
test split (`SPLIT_FEATURE_EXCLUDE` in the Makefile) and will not run under
`make feature_test`.

### Versions under test

| Where | Versions | Blocking |
| --- | --- | --- |
| `feature_odyssey` job in `tests.yaml`, on every pull request | 1.5.0, 1.5.1 | yes |
| `odyssey_versions` job in `nightly-odyssey.yaml` | the above plus `latest` and `master` | no |

Both lists are written out in their own workflow, so **adding or retiring a version means
editing both**. The nightly job prints the digest each moving tag resolved to, so a
nightly failure can still be tied to a specific Odyssey build.

The pull request matrix uses release tags rather than digests, which is enough while these
versions are the supported set. Switch it to `name:tag@sha256:...` if the job is ever made
a required check and full reproducibility is needed.

### Replaying the whole suite through Odyssey

Setting `SPQR_FEATURE_POOLER=odyssey` makes the harness send client SQL for the main
`router` through the pooler instead of directly, so the existing scenarios can be replayed
through Odyssey without being rewritten. The router admin console is never proxied, and
neither is `router2`, because the pooler has a single upstream. If a scenario selects a
compose profile that leaves the pooler out, the harness falls back to connecting to the
router directly and says so in the log.

The `feature_suite_through_odyssey` job in `nightly-odyssey.yaml` does this for the whole
suite. It is exploratory and does not fail the workflow: parts of the suite assume a direct
session with the router, so a red result is expected until the surviving subset is known.
Its purpose is to produce that subset.

## Troubleshooting

In case you are using Docker or Docker Desktop, everything should work fine. But if you are using Colima, you need to set up something before running the feature test.

> Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?

```bash
export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock
```

> Error response from daemon: client version 1.51 is too new. Maximum supported API version is 1.47, failed to setup compose cluster: Error response from daemon: client version 1.51 is too new. Maximum supported API version is 1.47

```bash
export DOCKER_API_VERSION=1.47
```

In case you are using Rancher Desktop 
> Error: failed to setup compose cluster: Cannot connect to the Docker daemon at unix:///var/run/docker. sock. Is the docker daemon running?

You need to enable the setting "Allow acquiring of administrative credentials (sudo access)" in the Preferences -> Application -> General section.


## Debug in the environment similar environment of feature tests on linux using VS Code
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
3. run test environment 
Example for run in cluster mode.
```shell
export ROUTER_CONFIG="/spqr/test/feature/conf/router_cluster.yaml" export COORDINATOR_CONFIG="/spqr/test/feature/conf/coordinator.yaml" export ROUTER_COORDINATOR_CONFIG="/spqr/test/feature/conf/coordinator.yaml"  export ROUTER_2_COORDINATOR_CONFIG="/spqr/test/feature/conf/coordinator.yaml";
docker compose --verbose -f ./test/feature/docker-compose-debug.yaml up
```
You can change environment variables like different behaviour like in feature-test scenarios. For example like `redistribute.feature` test:
```
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_three_shards.yaml
    COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator_three_shards.yaml
    """
```
You can run feature-test cluster without debug mode using `./test/feature/docker-compose.yaml` instead of `./test/feature/docker-compose-debug.yaml`. Cluster configuration in this `docker-compose.yaml` have 2 coordinators but only one can be active. You can run command `show coordinator_address;` to get address of active coordinator:
```
regress=> show coordinator_address;
    coordinator address     
----------------------------
 regress_coordinator_2:7003
(1 row)
```

4. Attach to the required spqr cluster components in the required order. Only the components you will attach to will work.

### Troubleshooting
- Fix toolchain version in .vscode\launch.json if you have problem in "Step into" action.