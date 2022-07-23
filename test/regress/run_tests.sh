set -x

../../spqr-rr run -c ./conf/regression.yaml > /dev/null 2>&1 &

spqr_pid=$(echo $!)
# XXX:bootstrap database
#psql ""

export PGOPTIONS='-c spqr_reply_shard_match=on'

# wait for spqr to start
sleep 2

rm -f regression.diffs
touch regression.diffs

for line in `cat schedule`
do
	psql "host=localhost port=6432 dbname=regression_spqr sslmode=disable" -f ./sql/$line.sql > ./results/$line.out 2>&1
	diff ./expected/$line.out ./results/$line.out >> regression.diffs
done


echo '

Tests exp/actual diffs: 

'

cat regression.diffs

kill $spqr_pid
