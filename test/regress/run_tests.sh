#!/bin/bash
set -x 

export PGDATABASE=regress
export PGUSER=regress
export PGSSLMODE=allow

source ./regress_utils.sh 

run_tests "console" "regress_coordinator" "7002"

run_tests "console" "regress_router" "7432"

# Topology variants (REGRESS_VARIANT=odyssey, see docker-compose-odyssey.yaml)
# run the very same router tests minus the cases listed in
# schedule/$REGRESS_VARIANT.skip.
ROUTER_SCHEDULE=router
if [ -n "${REGRESS_VARIANT:-}" ]; then
    ROUTER_SCHEDULE=$REGRESS_VARIANT
    build_schedule "router" "$ROUTER_SCHEDULE" "/regress/schedule/$REGRESS_VARIANT.skip"
fi

run_tests "router" "regress_router" "6432" "$ROUTER_SCHEDULE"

run_tests "pooler" "regress_pooler" "6432"

run_tests "coordinator" "regress_coordinator" "7002"

# test if diffs are empty

if test -s /regress/tests/combined.diffs; then
    exit 1
fi
exit 0
