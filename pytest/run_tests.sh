#!/bin/bash

set -x

env_prefix=oss
module_suffix=so

[[ -n "$1" ]] && env_prefix="$1"
[[ "$env_prefix" != "oss" ]] && module_suffix=zip

shift

run_tests() {
	shards=$1
	shift

	if [[ $shards == 0 ]]; then
		echo "no cluster on $env_prefix"
		RLTest --clear-logs --module ../redisgears.so --env $env_prefix "$@"
	else
		echo "cluster mode, $nodes shard"
		RLTest --clear-logs --module ../redisgears.$module_suffix --env $env_prefix-cluster --shards-count $shards "$@"
	fi
}

run_tests 0 "$@"
run_tests 1 "$@"
run_tests 2 "$@"
run_tests 3 "$@"
