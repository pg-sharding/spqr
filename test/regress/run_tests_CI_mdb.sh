#!/bin/bash
set -xEeuo pipefail

for UBUNTU_VERSION in focal jammy noble; do
    make POSTGRES_VERSION=13 codename=$UBUNTU_VERSION image=regress-mdb-image mdb-branch=MDB_13 shard-image=shard-image-mdb regress && \
    make POSTGRES_VERSION=14 codename=$UBUNTU_VERSION image=regress-mdb-image mdb-branch=MDB_14 shard-image=shard-image-mdb regress && \
    make POSTGRES_VERSION=15 codename=$UBUNTU_VERSION image=regress-mdb-image mdb-branch=MDB_15 shard-image=shard-image-mdb regress && \
    make POSTGRES_VERSION=16 codename=$UBUNTU_VERSION image=regress-mdb-image mdb-branch=MDB_16 shard-image=shard-image-mdb regress && \
    make POSTGRES_VERSION=17 codename=$UBUNTU_VERSION image=regress-mdb-image mdb-branch=MDB_17 shard-image=shard-image-mdb regress
done
