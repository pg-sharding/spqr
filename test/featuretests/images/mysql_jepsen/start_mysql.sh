#!/bin/bash

set -x
set -e
date

cat <<EOF > /etc/mysql/init.sql
   SET GLOBAL super_read_only = 0;
   CREATE USER $MYSQL_ADMIN_USER@'%' IDENTIFIED BY '$MYSQL_ADMIN_PASSWORD';
   GRANT ALL ON *.* TO $MYSQL_ADMIN_USER@'%' WITH GRANT OPTION;
   CREATE USER repl@'%' IDENTIFIED BY 'repl_pwd';
   GRANT REPLICATION SLAVE ON *.* TO repl@'%';
   CREATE DATABASE test1;
   CREATE USER 'client'@'%' IDENTIFIED BY 'client_pwd';
   GRANT ALL ON test1.* TO 'client'@'%';
   GRANT REPLICATION CLIENT ON *.* TO 'client'@'%';
   RESET MASTER;
   SET GLOBAL super_read_only = 1;
EOF

if [ ! -z "$MYSQL_MASTER" ]; then
cat <<EOF > /etc/mysql/slave.sql
    SET GLOBAL server_id = $MYSQL_SERVER_ID;
    RESET SLAVE FOR CHANNEL '';
    CHANGE MASTER TO MASTER_HOST = '$MYSQL_MASTER', MASTER_USER = 'repl', MASTER_PASSWORD = 'repl_pwd', MASTER_AUTO_POSITION = 1, MASTER_CONNECT_RETRY = 1, MASTER_RETRY_COUNT = 100500 FOR CHANNEL '';
    START SLAVE;
EOF
else
    touch /etc/mysql/slave.sql
fi

if [ ! -f /var/lib/mysql/auto.cnf ]; then
    /usr/sbin/mysqld --initialize --datadir=/var/lib/mysql --init-file=/etc/mysql/init.sql --server-id=$MYSQL_SERVER_ID
    echo "==INITIALIZED=="
else
    # clean slave script for restarts
    echo "" > /etc/mysql/slave.sql
fi

# workaround for docker on mac
chown -R mysql:mysql /var/lib/mysql
find /var/lib/mysql -type f -exec touch {} +

echo "==EMULATING SLOW START=="
sleep 10

echo "==STARTING=="
exec /usr/sbin/mysqld --defaults-file=/etc/mysql/my.cnf --datadir=/var/lib/mysql --init-file=/etc/mysql/slave.sql --server-id=$MYSQL_SERVER_ID --report-host=`hostname`
