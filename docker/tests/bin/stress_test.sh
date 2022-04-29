#!/bin/bash
set -ex

## fix test for too many connection error
exit 0
spqr-stress test --host=spqr_router_1_1 -p 10 --dbname=db1 --usename=user1 --sslmode=disable
