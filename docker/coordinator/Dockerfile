FROM spqr-base-image

RUN apt-get update && apt-get install -y postgresql-client

CMD [ "-c", "/spqr/docker/coordinator/cfg.yaml" ]
ENTRYPOINT [ "/spqr/spqr-coordinator" ]
