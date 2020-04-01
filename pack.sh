#!/bin/bash

error() {
	echo "There are errors."
	exit 1
}

trap error ERR

if [[ $1 == --help || $1 == help ]]; then
	cat <<-END
		Generate RedisGears distribution packages.
	
		[ARGVARS...] pack.sh [--help|help] [<module-so-path>]
		
		Argument variables:
		VERBOSE=1     Print commands
		IGNERR=1      Do not abort on error
		
		RAMP=1        Generate RAMP package
		DEPS=1        Generate dependency packages
		RELEASE=1     Generate "release" packages (artifacts/release/)
		SNAPSHOT=1    Generate "shapshot" packages (artifacts/snapshot/)
		JUST_PRINT=1  Only print package names, do not generate

		BRANCH=name         Branch name for snapshot packages
		GITSHA=1            Append Git SHA to shapshot package names
		CPYTHON_PREFIX=dir  Location of Python environment

	END
	exit 0
fi

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

	cd $ROOT

	local ramp="$(command -v python) -m RAMP.ramp" 
	local packfile=$PACKAGE_NAME.$OS-$OSNICK-$ARCH.$vertag.zip
	local packname=$(GEARS_NO_DEPS=1 $ramp pack -m ramp.yml -o $packfile $GEARS_SO | tail -1)

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
	# find . -name __pycache__ -type d -exec rm -rf {} \; 2>> /dev/null || true
	{ tar -c --sort=name --owner=root:0 --group=root:0 --mtime='UTC 1970-01-01' --transform "s,^./,$CPYTHON_PREFIX/," ./ 2>> /tmp/pack.err | gzip -n - > $TAR_PATH ; E=$?; } || true
	[[ $E != 0 ]] && cat /tmp/pack.err; $(exit $E)
	cd - > /dev/null
	sha256sum $TAR | gawk '{print $1}' > $TAR.sha256
	echo Created $TAR

	local TAR1=artifacts/snapshot/$SNAPSHOT_deps
	cp $TAR $TAR1
	cp $TAR.sha256 $TAR1.sha256
	# ( cd artifacts/snapshot; ln -sf ../../$TAR $TAR1; ln -sf ../../$TAR.sha256 $TAR1.sha256; )
	echo Created $TAR1
}

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
	if ! command -v redis-server > /dev/null; then
		echo Cannot find redis-server. Aborting.
		exit 1
	fi

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
