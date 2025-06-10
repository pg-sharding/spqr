# SPQR proto specs.


SPQR coordinator uses proto-spects to configure its routers metadata.

So, when user configures topology/data sharding throught router SQL interface, in
spqr-infra and spqr-coordinator setups, this SQL will be translated to proto-grpc coordinator call, which will then issue another proto-gprc calls to all other routers in a system. 