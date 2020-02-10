#!/bin/bash

PACK_RAMP=${PACK_RAMP:-1}
PACK_DEPS=${PACK_DEPS:-1}

[[ $VERBOSE == 1 ]] && set -x
[[ $IGNERR == 1 ]] || set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
. $HERE/deps/readies/shibumi/functions

ARCH=$(./deps/readies/bin/platform --arch)
OS=$(./deps/readies/bin/platform --os)
OSNICK=$(./deps/readies/bin/platform --osnick)

getver() {
	local getver_c=$(mktemp "${TMPDIR:-/tmp}"/getver-XXXXXXX.c)
	cat <<- EOF > $getver_c
		#include <stdio.h>

		#include "src/version.h"

		int main(int argc, char *argv[]) {
				printf("%d.%d.%d\n", REDISGEARS_VERSION_MAJOR, REDISGEARS_VERSION_MINOR, REDISGEARS_VERSION_PATCH);
				return 0;
		}
		EOF
	local getver=$(mktemp "${TMPDIR:-/tmp}"/getver.XXXXXXX)
	gcc -I. -o $getver $getver_c
	local ver=`$getver`
	rm -f $getver $getver_c
	echo $ver
}

pack() {
	local artifact="$1"
	local vertag="$2"

	local packfile=$PACKAGE_NAME.$OS-$OSNICK-$ARCH.$vertag.zip

	local packer=$(mktemp "${TMPDIR:-/tmp}"/pack.XXXXXXX)
	RAMP="$(command -v python) -m RAMP.ramp" 
	cat <<- EOF > $packer
		cd $ROOT
		$RAMP pack $GEARS -m ramp.yml -o $packfile | tail -1
	EOF

	cd $CPYTHON_PREFIX

	export LC_ALL=C.UTF-8
	export LANG=C.UTF-8
	
	packname=`pipenv run bash $packer`
	cd $ROOT
	[[ -f $packer ]] && rm -f $packer
	if [[ -z $packname ]]; then
		echo Failed to pack $artifact
		exit 1
	fi
	
	mv $packname artifacts/$artifact/
	packname="artifacts/$artifact/$packname"
	echo Created $packname
}

pack_deps() {
	CPYTHON_PREFIX=${CPYTHON_PREFIX:-/var/opt/redislabs/lib/modules/python3}
	if [[ ! -d $CPYTHON_PREFIX ]]; then
		echo $CPYTHON_PREFIX does not exist
		exit 1
	fi

	local TAR=${PACKAGE_NAME}-dependencies.$OS-$OSNICK-$ARCH.$VERSION.zip
	TAR_PATH=$(realpath artifacts/release/$TAR)
	cd $CPYTHON_PREFIX/
	{ tar pczf $TAR_PATH --transform "s,^./,$CPYTHON_PREFIX/," ./ 2>> /tmp/pack.err; E=$?; } || true
	[[ $E != 0 ]] && cat /tmp/pack.err; $(exit $E)
	cd - > /dev/null
	echo Created artifacts/release/$TAR

	local TAR1=${PACKAGE_NAME}-dependencies.$OS-$OSNICK-$ARCH.$BRANCH.zip
	cp artifacts/release/$TAR artifacts/snapshot/$TAR1
	echo Created artifacts/snapshot/$TAR1
}

if ! command -v redis-server > /dev/null; then
	echo Cannot find redis-server. Aborting.
	exit 1
fi

export ROOT=`git rev-parse --show-toplevel`

PACKAGE_NAME=${PACKAGE_NAME:-redisgears}
BRANCH=${CIRCLE_BRANCH:-`git rev-parse --abbrev-ref HEAD`}
BRANCH=${BRANCH//[^A-Za-z0-9._-]/_}
OS_VERSION=${OS_VERSION:-generic}

export PYTHONWARNINGS=ignore

cd $ROOT
mkdir -p artifacts/snapshot artifacts/release

VERSION=`getver`

if [[ $JUST_PRINT == 1 ]]; then
	if [[ $PACK_RAMP == 1 ]]; then
		[[ $RELEASE == 1 ]] && echo ${PACKAGE_NAME}.$OS-$OSNICK-$ARCH.$VERSION.zip
		[[ $SNAPSHOT == 1 ]] && echo ${PACKAGE_NAME}.$OS-$OSNICK-$ARCH.$BRANCH.zip
	fi
	if [[ $PACK_DEPS == 1 ]]; then
		[[ $RELEASE == 1 ]] && echo ${PACKAGE_NAME}-dependencies.$OS-$OSNICK-$ARCH.$VERSION.zip
		[[ $SNAPSHOT == 1 ]] && echo ${PACKAGE_NAME}-dependencies.$OS-$OSNICK-$ARCH.$BRANCH.zip
	fi
	exit 0
fi

if [[ $PACK_RAMP == 1 ]]; then
	[[ -z $1 ]] && echo Nothing to pack. Aborting. && exit 1
	[[ ! -f $1 ]] && echo $1 does not exist. Aborting. && exit 1
	GEARS=$(realpath $1)
	RELEASE_GEARS=$GEARS
	SNAPSHOT_GEARS=$(dirname $GEARS)/snapshot/$(basename $GEARS)
fi

if [[ $PACK_RAMP == 1 ]]; then
	GEARS=$RELEASE_GEARS pack release "{semantic_version}"
	GEARS=$SNAPSHOT_GEARS pack snapshot "$BRANCH"
fi

if [[ $PACK_DEPS == 1 ]]; then
	pack_deps
fi
