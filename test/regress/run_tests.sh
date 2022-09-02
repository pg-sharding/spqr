#!/bin/bash

set -x

LOGFILE=router.log
echo 'Router tests:' >> LOGFILE

# wait for spqr to start
sleep 2

rm -f regression.diffs
touch regression.diffs

mkdir ./results
while IFS= read -r line
do
  psql "host=regress_router_1 port=6432 dbname=regress user=regress sslmode=disable" -f sql/"$line".sql > ./results/"$line".out 2>&1
  diff ./expected/"$line".out ./results/"$line".out >> regression.diffs
done < ./schedule

if [ -s regression.diffs ]; then
	echo '

	Tests exp/actual diffs: 

	'
	cat regression.diffs

	exit 2
fi
