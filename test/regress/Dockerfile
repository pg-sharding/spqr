ARG image
FROM ${image:-regress-base-image}

ARG POSTGRES_VERSION=13

USER root

ENV PATH=$PATH:"/usr/lib/postgresql/${POSTGRES_VERSION}/lib/pgxs/src/test/regress:/home/build-user/pgbin/lib/postgresql/pgxs/src/test/regress"

COPY ./ /regress
RUN chmod a+x /regress/run_tests.sh
WORKDIR "/regress"

ENTRYPOINT "./run_tests.sh"
