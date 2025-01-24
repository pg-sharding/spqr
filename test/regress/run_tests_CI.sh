#!/bin/bash
set -ex

for UBUNTU_VERSION in focal jammy noble; do
    make POSTGRES_VERSION=12 codename=$UBUNTU_VERSION regress
    make POSTGRES_VERSION=13 codename=$UBUNTU_VERSION regress
    make POSTGRES_VERSION=14 codename=$UBUNTU_VERSION regress
    make POSTGRES_VERSION=15 codename=$UBUNTU_VERSION regress
    make POSTGRES_VERSION=16 codename=$UBUNTU_VERSION regress
    make POSTGRES_VERSION=17 codename=$UBUNTU_VERSION regress
    make POSTGRES_VERSION=13 codename=$UBUNTU_VERSION image=regress-mdb-image regress
    make POSTGRES_VERSION=14 codename=$UBUNTU_VERSION image=regress-mdb-image regress
    make POSTGRES_VERSION=15 codename=$UBUNTU_VERSION image=regress-mdb-image regress
    make POSTGRES_VERSION=16 codename=$UBUNTU_VERSION image=regress-mdb-image regress
    make POSTGRES_VERSION=17 codename=$UBUNTU_VERSION image=regress-mdb-image regress
done
