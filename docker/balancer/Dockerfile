FROM spqr-base-image

# TODO: remove lines
ADD /docker/balancer/cfg.yaml /spqr/docker/balancer/cfg.yaml
ADD /spqr-balancer /spqr/spqr-balancer

ENTRYPOINT /spqr/spqr-balancer run -c /spqr/docker/balancer/cfg.yaml
