#!/bin/bash

[[ $V == 1 || $VERBOSE == 1 ]] && set -x
[[ $IGNERR == 1 ]] || set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/.. && pwd)
READIES=$ROOT/deps/readies
. $READIES/shibumi/defs

OP=""
[[ $NOP == 1 ]] && OP=echo

[[ -z $MOD ]] && MOD="$ROOT/redisgears.so"
if [[ ! -f $MOD ]]; then
	eprint "No Gears module at $MOD"
	exit 1
fi

[[ -z $GEARSPY_PATH ]] && GEARSPY_PATH=$ROOT/gears_python.so
if [[ ! -f $GEARSPY_PATH ]]; then
	eprint "No Gears Python plugin at $GEARSPY_PATH"
	exit 1
fi

export ENV_PREFIX=${ENV_PREFIX:-oss}
module_suffix=".so"

[[ $ENV_PREFIX != oss ]] && module_suffix=".zip"

if [[ $VALGRIND == 1 ]]; then
	MORE_ARGS="--use-valgrind --vg-suppressions ../leakcheck.supp"
else
	MORE_ARGS=""
fi

if [[ $TLS == 1 ]]; then
	$HERE/generate_tests_cert.sh
	MORE_ARGS+=" --tls --tls-cert-file ./tests/tls/redis.crt --tls-key-file ./tests/tls/redis.key --tls-ca-cert-file ./tests/tls/ca.crt --tls-passphrase foobar"
fi

run_tests() {
	local shards=$1
	shift

	if [[ $shards == 0 ]]; then
		echo "No cluster on $ENV_PREFIX"
		local env="$ENV_PREFIX"
	else
		echo "Cluster mode, $shards shard"
		MOD=${MOD/.so/$module_suffix}
		local shards_arg="--shards-count $shards"
		local env="${ENV_PREFIX}-cluster"
	fi
	$OP python3 -m RLTest --clear-logs --module $MOD --module-args "Plugin $GEARSPY_PATH" --env $env $shards_arg $MORE_ARGS $TEST_ARGS "$@"
}

cd $HERE
mkdir -p logs
if [[ -z $SHARDS ]]; then
	run_tests 0 "$@"
	run_tests 1 "$@"
	run_tests 2 "$@"
	run_tests 3 "$@"
else
	run_tests $SHARDS "$@"
fi
