#!/bin/bash

pack() {
	local artifact="$1"
	local branch="$2"

	local packfile=$PACKAGE_NAME.{os}-$OS_VERSION-{architecture}.$branch.zip
	local log=pack/$artifact.log

	local packer=$(mktemp --tmpdir pack.XXXXXXX)
	cat <<- EOF > $packer
		cd $ROOT
		ramp pack $GEARS -m ramp.yml -o $packfile 2> $log | tail -1
	EOF

	cd $CPYTHON_PREFIX
	packname=`pipenv run bash $packer 2>> $artifact.log`
	cd $ROOT
	[[ -f $packer ]] && rm -f $packer
	if [[ -z $packname ]]; then
		echo Failed to pack $artifact
		exit 1
	fi
	
	mv $packname artifacts/$artifact/
	packname="artifacts/$artifact/$packname"
	echo Created $packname
	rm -f $log
}

set -e

if ! command -v redis-server > /dev/null; then
	echo Cannot find redis-server. Aborting.
	exit 1
fi

export ROOT=`git rev-parse --show-toplevel`
CPYTHON_PREFIX=${CPYTHON_PREFIX:-/opt/redislabs/lib/modules/python27}
GEARS=redisgears.so

PACKAGE_NAME=${PACKAGE_NAME:-redisgears}
BRANCH=${CIRCLE_BRANCH:-`git rev-parse --abbrev-ref HEAD`}
OS_VERSION=${OS_VERSION:-linux}

export PYTHONWARNINGS=ignore

cd $ROOT
mkdir -p artifacts/snapshot artifacts/release

pack release "{semantic_version}"
RELEASE=$packname

pack snapshot "$BRANCH"

if [[ ! -d $CPYTHON_PREFIX ]]; then
	echo $CPYTHON_PREFIX does not exist
	exit 1
fi

stem=$(basename $RELEASE | sed -e "s/^$PACKAGE_NAME\.\(.*\)\.zip/\1/")
TAR=$PACKAGE_NAME-dependencies.$stem.tgz
tar pczf artifacts/release/$TAR $CPYTHON_PREFIX/ 2> /dev/null
echo Created artifacts/release/$TAR
