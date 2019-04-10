#!/bin/bash

env_prefix=oss
[[ -n "$1" ]] && env_prefix="$1"

shift

./run_tests.sh $env_prefix --use-valgrind --vg-suppressions ../leakcheck.supp "$@"
