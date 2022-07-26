#!/bin/bash

set -ex

sleep 25
console.sh

simple.sh

move.sh

coordinator_setup.sh

with_coordinator.sh
