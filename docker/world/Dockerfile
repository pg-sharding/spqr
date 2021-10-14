
FROM golang:1.17 as worldbase

RUN mkdir /router
COPY ./ /router

RUN cd /router && make init && make build_world

FROM spqr_shardbase

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
