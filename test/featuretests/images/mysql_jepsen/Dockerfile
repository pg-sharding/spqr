FROM mysync-jepsen-test-base:latest
COPY . /var/lib/dist/mysql
COPY ./sh-scripts/my-wait-started.sh /usr/bin/my-wait-started
COPY ./mysync /usr/bin/mysync
COPY ./sh-scripts/my-resetup.sh /usr/bin/my-resetup
COPY ./sh-scripts/my-resetup-wd.sh /usr/bin/my-resetup-wd
COPY ./setup.sh /usr/local/bin/setup.sh
COPY ./my.cnf /etc/mysql/my.cnf
COPY ./.my.cnf /root/.my.cnf
COPY ./supervisor_mysql.conf /etc/supervisor/conf.d
RUN chown mysql:root /etc/mysql
RUN touch /etc/mysync.yaml
RUN chown mysql:mysql /etc/mysync.yaml

RUN chmod +x /usr/bin/my-resetup

RUN echo 'APT::Install-Recommends "0"; \n\
APT::Get::Assume-Yes "true"; \n\
APT::Get::force-yes "true"; \n\
APT::Install-Suggests "0";' > /etc/apt/apt.conf.d/01buildconfig && \
    apt-get update && apt-get install wget gnupg ca-certificates && \
    apt-get update && \
    apt-get install 

ENV MYSQL_ADMIN_USER admin
ENV MYSQL_ADMIN_PASSWORD admin_pwd
