FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive
COPY --from=networld/grpcurl /grpcurl /usr/bin/
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends wget vim git build-essential ca-certificates \
    libreadline-dev zlib1g-dev bison flex postgresql postgresql-contrib
RUN git clone -b REL_13_STABLE --single-branch https://github.com/postgres/postgres.git

WORKDIR /build/postgres
RUN ./configure && make
RUN mkdir -p /usr/local/pgsql/bin/ && ln -s /usr/bin/psql /usr/local/pgsql/bin/psql

ENV LANG=C.UTF-8 PGHOST=installcheck_router_1 PGPORT=6432 PGSSLMODE=disable BUILDKIT_PROGRESS=plain
RUN chmod -R go+rwX /build
USER postgres
CMD ["make", "installcheck"]
