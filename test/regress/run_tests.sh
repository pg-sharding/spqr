set -x

../../spqr-rr run -c ./conf/regression.yaml > /dev/null 2>&1 &

spqr_pid=$(echo $!)
# XXX:bootstrap database
#psql ""

# wait for spqr to start
sleep 2


for line in `cat schedule`
do
	psql "host=localhost port=6432 dbname=regression_spqr sslmode=disable" -f ./sql/$line.sql > ./results/$line.out 2>&1
done

kill $spqr_pid
