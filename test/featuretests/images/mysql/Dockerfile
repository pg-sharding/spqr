ARG VERSION=""
FROM mysync-test-base${VERSION}:latest
ARG VERSION=""
ENV VERSION="${VERSION}"
COPY . /var/lib/dist/mysql
COPY ./mysync /usr/bin/mysync
RUN bash /var/lib/dist/mysql/setup.sh
