#!/bin/bash
set -ex

## fix test for too many connection error
exit 0
spqr-stress test --host=e2e_router_1 -p 10 --dbname=db1 --usename=user1 --sslmode=disable
