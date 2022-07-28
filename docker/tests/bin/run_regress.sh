set -x

# wait for spqr to start
sleep 2

rm -f regression.diffs
touch regression.diffs

while IFS= read -r line
do
	psql "host=localhost port=7002 dbname=regression_spqr sslmode=disable" -f ./coordregress/"$line".sql > ./results/"$line".out 2>&1
	diff ./expected/"$line".out ./results/"$line".out >> regression.diffs
done < coordregress/schedule

if [ -s regression.diffs ]; then
	echo '
	Tests exp/actual diffs:

	'
	cat regression.diffs

	exit 2
fi


