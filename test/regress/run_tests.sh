#set -x

LOGFILE=log.log

../../spqr-rr run -c ./conf/regression-tx.yaml > $LOGFILE 2>&1 &

spqr_pid=$!
# XXX:bootstrap database
#psql ""

export PGOPTIONS='-c spqr_reply_shard_match=on'

# wait for spqr to start
sleep 2

rm -f regression.diffs
touch regression.diffs


while IFS= read -r line
do
  psql "host=localhost port=6433 dbname=regression_spqr sslmode=disable" -f ./sql/"$line".sql > ./results/"$line".out 2>&1
  diff ./expected/"$line".out ./results/"$line".out >> regression.diffs
done < schedule

kill "$spqr_pid"

if [ -s regression.diffs ]; then
	echo '

	Tests exp/actual diffs: 

	'
	cat regression.diffs

	exit 2
fi


