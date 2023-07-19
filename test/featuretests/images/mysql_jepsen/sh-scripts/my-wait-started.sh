#!/bin/bash

WAIT_TIME=120

for i in `seq 1 $WAIT_TIME`
do
	if mysql -e 'select 1'; then
		exit 0
	fi
	sleep 1
done

exit 1
