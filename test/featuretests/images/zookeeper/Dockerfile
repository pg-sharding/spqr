ARG VERSION=""
FROM mysync-test-base${VERSION}:latest
COPY . /var/lib/dist/zookeeper
RUN bash /var/lib/dist/zookeeper/setup.sh
