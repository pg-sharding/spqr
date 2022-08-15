FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Israel
ENV LD_LIBRARY_PATH=/usr/local/pgsql/lib
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    lsb-release \
    ca-certificates \
    libssl-dev \
    libldap-common \
    gnupg \
    openssl \
    ldap-utils \
    libldap-2.4-2 \
    libldap-dev

RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    libreadline-dev \
    zlib1g-dev \
    flex \
    bison \
    postgresql-common \
    postgresql-14
# RUN /usr/local/lib/postgresql/pgxs/src/test/regress/pg_regress

RUN git clone http://github.com/postgres/postgres --depth 1 -b REL_14_STABLE
WORKDIR /postgres
RUN ./configure && make
RUN mkdir -p /usr/local/pgsql/bin/ && ln -s /usr/bin/psql /usr/local/pgsql/bin/psql
# RUN make installcheck

ENTRYPOINT "/bin/bash"
