#!/bin/bash

RAMP=${RAMP:-1}
DEPS=${DEPS:-1}

RELEASE=${RELEASE:-1}
SNAPSHOT=${SNAPSHOT:-1}

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
	local prog=$(mktemp "${TMPDIR:-/tmp}"/getver.XXXXXXX)
	gcc -I. -o $prog $getver_c
	local ver=`$prog`
	rm -f $prog $getver_c
	echo $ver
}

pack() {
	local artifact="$1"
	local vertag="$2"

	local packfile=$PACKAGE_NAME.$OS-$OSNICK-$ARCH.$vertag.zip

	local packer=$(mktemp "${TMPDIR:-/tmp}"/pack.XXXXXXX)
	ramp="$(command -v python) -m RAMP.ramp" 
	cat <<- EOF > $packer
		cd $ROOT
		$ramp pack $GEARS_SO -m ramp.yml -o $packfile | tail -1
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
	echo "Created artifacts/$artifact/$packname"
}

pack_deps() {
	CPYTHON_PREFIX=${CPYTHON_PREFIX:-/var/opt/redislabs/lib/modules/python3}
	if [[ ! -d $CPYTHON_PREFIX ]]; then
		echo $CPYTHON_PREFIX does not exist
		exit 1
	fi

	local TAR=artifacts/release/$RELEASE_deps
	TAR_PATH=$(realpath $TAR)
	cd $CPYTHON_PREFIX/
	{ tar pczf $TAR_PATH --transform "s,^./,$CPYTHON_PREFIX/," ./ 2>> /tmp/pack.err; E=$?; } || true
	[[ $E != 0 ]] && cat /tmp/pack.err; $(exit $E)
	cd - > /dev/null
	sha256sum $TAR | gawk '{print $1}' > $TAR.sha256
	echo Created $TAR

	local TAR1=$SNAPSHOT_deps
	# cp artifacts/release/$TAR artifacts/snapshot/$TAR1
	( cd artifacts/snapshot; ln -sf ../../$TAR $TAR1; ln -sf ../../$TAR.sha256 $TAR1.sha256; )
	echo Created artifacts/snapshot/$TAR1
}

if ! command -v redis-server > /dev/null; then
	echo Cannot find redis-server. Aborting.
	exit 1
fi

# export ROOT=`git rev-parse --show-toplevel`
export ROOT=$(realpath $HERE)

PACKAGE_NAME=${PACKAGE_NAME:-redisgears}

[[ -z $BRANCH ]] && BRANCH=${CIRCLE_BRANCH:-`git rev-parse --abbrev-ref HEAD`}
BRANCH=${BRANCH//[^A-Za-z0-9._-]/_}
if [[ $GITSHA == 1 ]]; then
	GIT_COMMIT=$(git describe --always --abbrev=7 --dirty="+" 2>/dev/null || git rev-parse --short HEAD)
	BRANCH="${BRANCH}-${GIT_COMMIT}"
fi

export PYTHONWARNINGS=ignore

cd $ROOT

VERSION=$(getver)

RELEASE_ramp=${PACKAGE_NAME}.$OS-$OSNICK-$ARCH.$VERSION.zip
SNAPSHOT_ramp=${PACKAGE_NAME}.$OS-$OSNICK-$ARCH.$BRANCH.zip
RELEASE_deps=${PACKAGE_NAME}-dependencies.$OS-$OSNICK-$ARCH.$VERSION.tgz
SNAPSHOT_deps=${PACKAGE_NAME}-dependencies.$OS-$OSNICK-$ARCH.$BRANCH.tgz

if [[ $JUST_PRINT == 1 ]]; then
	if [[ $RAMP == 1 ]]; then
		[[ $RELEASE == 1 ]] && echo $RELEASE_ramp
		[[ $SNAPSHOT == 1 ]] && echo $SNAPSHOT_ramp
	fi
	if [[ $DEPS == 1 ]]; then
		[[ $RELEASE == 1 ]] && echo $RELEASE_deps
		[[ $SNAPSHOT == 1 ]] && echo $SNAPSHOT_deps
	fi
	exit 0
fi

mkdir -p artifacts/snapshot artifacts/release

if [[ $RAMP == 1 ]]; then
	[[ -z $1 ]] && echo Nothing to pack. Aborting. && exit 1
	[[ ! -f $1 ]] && echo $1 does not exist. Aborting. && exit 1
	
	RELEASE_SO=$(realpath $1)
	SNAPSHOT_SO=$(dirname $RELEASE_SO)/snapshot/$(basename $RELEASE_SO)
fi

if [[ $RAMP == 1 ]]; then
	GEARS_SO=$RELEASE_SO pack release "{semantic_version}"
	GEARS_SO=$SNAPSHOT_SO pack snapshot "$BRANCH"
fi

if [[ $DEPS == 1 ]]; then
	pack_deps
fi
