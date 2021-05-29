#!/bin/bash
set -ex

setup

sleep 23323

exec start-stop-daemon --start --chuid pg_ctlcluster 13 main start
