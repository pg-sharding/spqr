#!/bin/bash

export PGDATABASE=stress
export PGUSER=stress
export PGSSLMODE=disable
export PGPORT=6432


NUM_CLIENTS=10
NUM_TRANSACTIONS=1000
TEST_DURATION=60

sleep 10

echo "======== GENERATING TEST DATA ========"
pgbench -i -h spqr_shard_1 -U $PGUSER $PGDATABASE
pgbench -i -h spqr_shard_2 -U $PGUSER $PGDATABASE

echo "============= SHOW SCRIPT ============"
pgbench --show-script select-only

echo "========= RUN AGAINST ROUTER ========="
pgbench --builtin select-only -c $NUM_CLIENTS -T $TEST_DURATION -n -h stress_router -U $PGUSER $PGDATABASE

echo "======== RUN AGAINST POSTGRES ========"
pgbench --builtin select-only -c $NUM_CLIENTS -T $TEST_DURATION -n -h spqr_shard_1 -U $PGUSER $PGDATABASE


# TODO
# run select-only script
# pgbench --show-script simple-update
# pgbench --builtin simple-update -c $NUM_CLIENTS -T $TEST_DURATION -n -h $PGHOST -p $PGPORT -U $PGUSER $PGDATABASE

exit 0
