FROM spqr_spqrbase

# TODO: remove lines
ADD /docker/balancer/cfg.yaml /router/docker/balancer/cfg.yaml
ADD /spqr-balancer /router/spqr-balancer

ENTRYPOINT /router/spqr-balancer run -c /router/docker/balancer/cfg.yaml
