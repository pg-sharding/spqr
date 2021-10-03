#FROM ubuntu:focal
FROM golang:1.16

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Moskow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && apt-get install -y --no-install-recommends \
	curl \
	lsb-release \
	make \
	ca-certificates \
	gnupg \
	openssl

RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

RUN apt-get update && apt-get install -y --no-install-recommends \
    sudo postgresql-13 \
    vim 

COPY ./docker/tests/bin/ /usr/local/bin/
RUN mkdir /root/stress
COPY ./docker/tests/stress /root/stress
RUN make -C /root/stress build install

RUN chmod a+x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
