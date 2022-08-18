FROM ubuntu:20.04

WORKDIR /root

COPY . /root

RUN apt-get update \
&& apt-get install -y \
    make postgresql-client curl \
    gcc g++ \
&& curl -O https://dl.google.com/go/go1.18.5.linux-amd64.tar.gz  \
&& tar -C /usr/local -xzf go1.18.5.linux-amd64.tar.gz  \
&& export PATH=$PATH:/usr/local/go/bin\
&& make build

CMD ["make", "check"]
