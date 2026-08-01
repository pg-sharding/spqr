# syntax=docker/dockerfile:1
#
# Thin shard image built on top of docker/shard/Dockerfile.base.
#
# The base image carries all the heavy, rarely-changing bits (apt deps + the
# four external PostgreSQL extensions). Here we only add the runtime entrypoint
# and test config, so this layer is cheap to (re)build on every commit.
#
# BASE_IMAGE points to a prebuilt base image (e.g. pulled from GHCR in CI). When
# not provided it defaults to the locally built `spqr-shard-base` image, which
# the Makefile builds via docker/shard/Dockerfile.base.
ARG BASE_IMAGE=spqr-shard-base
FROM ${BASE_IMAGE}

ENV POSTGRES_USER=$POSTGRES_USER
ENV POSTGRES_DB=$POSTGRES_DB
ARG POSTGRES_VERSION=15
ENV POSTGRES_VERSION=$POSTGRES_VERSION

COPY ./docker/shard/bin/ /usr/local/bin/

RUN chmod a+x /usr/local/bin/entrypoint.sh

ENTRYPOINT /usr/local/bin/entrypoint.sh $POSTGRES_VERSION
