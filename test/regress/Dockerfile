ARG codename=jammy
FROM ubuntu:${codename}

ARG POSTGRES_VERSION=13

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Israel
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt update && apt install -y ca-certificates
RUN sed -i 's/archive.ubuntu.com/mirror.yandex.ru/g' /etc/apt/sources.list

RUN apt-get update -o Acquire::AllowInsecureRepositories=true && apt-get install -y --no-install-recommends --allow-unauthenticated \
	curl lsb-release ca-certificates gnupg

RUN install -d /usr/share/postgresql-common/pgdg && \
    curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list

RUN apt-get update && apt-get install -y --no-install-recommends \
    sudo postgresql-${POSTGRES_VERSION}

ENV PATH=$PATH:"/usr/lib/postgresql/${POSTGRES_VERSION}/lib/pgxs/src/test/regress:/home/build-user/pgbin/lib/postgresql/pgxs/src/test/regress"

COPY ./ /regress
RUN chmod a+x /regress/run_tests.sh
WORKDIR "/regress"

ENTRYPOINT "./run_tests.sh"
