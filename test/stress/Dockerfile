FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Israel
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && apt-get install -y --no-install-recommends \
	curl lsb-release ca-certificates gnupg

RUN install -d /usr/share/postgresql-common/pgdg && \
    curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list

RUN apt-get update && apt-get install -y --no-install-recommends \
    sudo postgresql-13 

ENV PATH=$PATH:"/usr/lib/postgresql/13/lib/pgxs/src/test/stress"

COPY ./ /stress
RUN chmod a+x /stress/run_tests.sh
WORKDIR "/stress"

ENTRYPOINT "./run_tests.sh"
