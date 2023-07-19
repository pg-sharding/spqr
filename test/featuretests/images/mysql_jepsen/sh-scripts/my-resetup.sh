#!/bin/bash

set -xe

RESETUP_FILE=/tmp/mysync.resetup
MYSQL_DATA_DIR=/var/lib/mysql
PATH_MASTER=/test/master

function zk_get() {
  echo "addauth digest testuser:testpassword123
  get /test/master" > /tmp/zk_commands

	cat /tmp/zk_commands | /opt/zookeeper/bin/zkCli.sh -server "mysync_zookeeper1_1.mysync_mysql_net:2181,mysync_zookeeper2_1.mysync_mysql_net:2181,mysync_zookeeper3_1.mysync_mysql_net:2181" | grep -o '"mysync_mysql[1-3]_1"' | grep -o '[a-z0-9_]*'
}


function mysql_set_gtid_purged() {
    gtids=$(tr -d '\n' < /var/lib/mysql/xtrabackup_binlog_info | awk '{print $3}')
    mysql -e "RESET MASTER; SET @@GLOBAL.GTID_PURGED='$gtids';"
}


function do_resetup() {
	# shutdown mysql
	if ! supervisorctl stop mysqld; then
		echo `date` failed to stop mysql
		return 1
	fi
	# cleanup datadir
	if ! find /var/lib/mysql/ -name "*" | egrep -v -e "auto\.cnf" -e "^/var/lib/mysql/$" | xargs rm -fr; then
		echo `date` failed to cleanup mysql dir
	fi

	echo `date` cleared up mysql dir

	master=$(zk_get $PATH_MASTER)
	echo `date` current master from zk: $master
	# fetch backup
	
	echo `date` fetching backup
	ssh root@"$master" "xtrabackup --backup --stream=xbstream" | sudo -u mysql xbstream --extract --directory="$MYSQL_DATA_DIR"
	chown -R mysql:mysql "$MYSQL_DATA_DIR"
	echo `date` preparing backup
	sudo -u mysql xtrabackup --prepare --target-dir="$MYSQL_DATA_DIR"
	
	echo `date` starting mysql
	supervisorctl start mysqld
	my-wait-started

	echo `date` setting gtid_purged
	mysql_set_gtid_purged

    echo `date` done
}


if [ -f $RESETUP_FILE ]; then
	do_resetup
	rm -fr $RESETUP_FILE
else
	echo `date` no resetup file found, delaying
fi

