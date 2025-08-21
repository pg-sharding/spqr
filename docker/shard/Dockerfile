ARG codename=jammy
FROM ubuntu:${codename}

ENV POSTGRES_USER=$POSTGRES_USER
ENV POSTGRES_DB=$POSTGRES_DB
ARG POSTGRES_VERSION=15
ENV POSTGRES_VERSION=$POSTGRES_VERSION

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt update && apt install -y ca-certificates
# RUN sed -i 's/archive.ubuntu.com/mirror.yandex.ru/g' /etc/apt/sources.list

RUN apt-get update -o Acquire::AllowInsecureRepositories=true && apt-get install -y --no-install-recommends --allow-unauthenticated \
    curl \
    lsb-release \
    ca-certificates \
    libssl-dev \
    libldap-common \
    gnupg \
    openssl

RUN install -d /usr/share/postgresql-common/pgdg && \
    curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    sudo postgresql-$POSTGRES_VERSION \
    build-essential \
    cmake \
    gcc \
    gdb \
    libpam0g-dev \
    valgrind \
    libpq5 \
    libpq-dev \
    vim \
    postgresql-common \
    postgresql-server-dev-$POSTGRES_VERSION \
    postgresql-${POSTGRES_VERSION}-pg-stat-kcache \
    git \
    python3-docutils

COPY ./docker/shard/bin/ /usr/local/bin/

RUN chmod a+x /usr/local/bin/entrypoint.sh

RUN git clone https://github.com/munakoiso/pg_comment_stats.git && \
    cd pg_comment_stats && \
    make && make install && \
    cd .. && git clone https://github.com/pg-sharding/spqrguard.git && \
    cd spqrguard && \
    make USE_PGXS=1 && make install && \
    cd .. && git clone https://github.com/pg-sharding/spqrhash.git && \
    cd spqrhash && \
    make install

ENTRYPOINT /usr/local/bin/entrypoint.sh $POSTGRES_VERSION
