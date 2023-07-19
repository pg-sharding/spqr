FROM mysync-jepsen-test-base:latest
COPY . /var/lib/dist/zookeeper
COPY ./supervisor_zookeeper.conf /etc/supervisor/conf.d
COPY ./retriable_path_create.sh /usr/local/bin/retriable_path_create.sh
COPY ./generate_certs_with_restart.sh /usr/local/bin/generate_certs_with_restart.sh
RUN chmod 755 /usr/local/bin/generate_certs_with_restart.sh
