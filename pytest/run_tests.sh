#!/bin/bash

[[ $VERBOSE == 1 ]] && set -x
[[ $IGNERR == 1 ]] || set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
. $HERE/../deps/readies/shibumi/functions

ROOT=$(realpath $HERE/..)

export ENV_PREFIX=${ENV_PREFIX:-oss}
module_suffix=so

[[ $ENV_PREFIX != oss ]] && module_suffix=zip

if [[ $VALGRIND == 1 ]]; then
	MORE_ARGS="--use-valgrind --vg-suppressions ../leakcheck.supp"
else
	MORE_ARGS=""
fi

run_tests() {
	local shards=$1
	shift

	if [[ $shards == 0 ]]; then
		echo "no cluster on $ENV_PREFIX"
		local mod="$ROOT/redisgears.so"
		local env="$ENV_PREFIX"
	else
		echo "cluster mode, $shards shard"
		local mod="$ROOT/redisgears.$module_suffix"
		local shards_arg="--shards-count $shards"
		local env="${ENV_PREFIX}-cluster"
	fi
	python -m RLTest --clear-logs --module $mod --env $env $shards_arg $MORE_ARGS "$@"
}

cd $HERE
mkdir -p logs
run_tests 0 "$@"
run_tests 1 "$@"
run_tests 2 "$@"
run_tests 3 "$@"
