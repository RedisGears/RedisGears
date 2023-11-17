#!/bin/bash

[[ $V == 1 || $VERBOSE == 1 ]] && set -x
[[ $IGNERR == 1 ]] || set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$HERE
export READIES=$ROOT/deps/readies
. $READIES/shibumi/defs

cd $ROOT

export PYTHONWARNINGS=ignore
export LC_CTYPE=en_US.utf-8


#----------------------------------------------------------------------------------------------

if [[ $1 == --help || $1 == help ]]; then
	cat <<-END
		Generate RedisGears distribution packages.

		[ARGVARS...] pack.sh [--help|help] [<module-so-path>]

		Argument variables:
		VERBOSE=1     Print commands
		IGNERR=1      Do not abort on error

		RAMP=1        Generate RAMP package
		GEARSPY=1     Generate Gears Python plugin package
		SYM=1         Generate packages of debug symbols
		RELEASE=1     Generate "release" packages (artifacts/release/)
		SNAPSHOT=1    Generate "shapshot" packages (artifacts/snapshot/)
		JUST_PRINT=1  Only print package names, do not generate

		VARIANT=name        Build variant (empty for standard packages)
		BRANCH=name         Branch name for snapshot packages (also: GEARS_PACK_BRANCH=name)
		GITSHA=1            Append Git SHA to shapshot package names
		CPYTHON_PREFIX=dir  Python install dir
		GEARSPY_PATH=path   Path of gears_python.so

	END
	exit 0
fi

#----------------------------------------------------------------------------------------------

MOD=$1

RAMP=${RAMP:-1}
GEARSPY=${GEARSPY:-0}
SYM=${SYM:-0}

RELEASE=${RELEASE:-1}
SNAPSHOT=${SNAPSHOT:-1}

ARCH=$($READIES/bin/platform --arch)
[[ $ARCH == x64 ]] && ARCH=x86_64

OS=$($READIES/bin/platform --os)
[[ $OS == linux ]] && OS=Linux

OSNICK=$($READIES/bin/platform --osnick)
[[ $OSNICK == trusty ]]  && OSNICK=ubuntu14.04
[[ $OSNICK == xenial ]]  && OSNICK=ubuntu16.04
[[ $OSNICK == bionic ]]  && OSNICK=ubuntu18.04
[[ $OSNICK == focal ]]   && OSNICK=ubuntu20.04
[[ $OSNICK == jammy ]]   && OSNICK=ubuntu22.04
[[ $OSNICK == centos7 ]] && OSNICK=rhel7
[[ $OSNICK == centos8 ]] && OSNICK=rhel8
[[ $OSNICK == ol8 ]]     && OSNICK=rhel8
[[ $OSNICK == rocky8 ]]  && OSNICK=rhel8

[[ $OSNICK == bigsur ]]  && OSNICK=catalina

PYTHON_BIN=python3
OS_DESC=$(${PYTHON_BIN} $ROOT/getos.py)

#----------------------------------------------------------------------------------------------

pack() {
	# artifact=release|snapshot
	local artifact="$1"
	local pack_fname="$2"

	cd $ROOT
	local packfile=artifacts/$artifact/$pack_fname
	if [[ -z $3 ]]; then
		${PYTHON_BIN} $READIES/bin/xtx \
			-d GEARS_PYTHON_NAME=python3_$SEMVER \
			-d GEARS_PYTHON_FNAME=$URL_FNAME \
			-d GEARS_PYTHON_SHA256=$(cat $GEARSPY_PKG.sha256) \
			-d GEARS_JAVA_FNAME=$JAVA_URL_FNAME \
			-d GEARS_JAVA_SHA256=$(cat $GEARSJVM_PKG.sha256) \
			-d OS_DESC=$OS_DESC \
			ramp.yml > /tmp/ramp.yml
	else
		${PYTHON_BIN} $READIES/bin/xtx \
			-d GEARS_PYTHON_NAME=python3_$SEMVER \
			-d GEARS_PYTHON_FNAME=$URL_FNAME \
			-d GEARS_PYTHON_SHA256=$(cat $GEARSPY_PKG.sha256) \
			-d OS_DESC=$OS_DESC \
			ramp_python.yml > /tmp/ramp.yml
	fi
	rm -f /tmp/ramp.fname
	if [[ ! -z $GEARSPY_PATH ]]; then
		GEARS_OPT="Plugin $GEARSPY_PATH"
	fi
	GEARS_NO_DEPS=1 ${PYTHON_BIN} -m RAMP.ramp pack -m /tmp/ramp.yml --packname-file /tmp/ramp.fname \
		--verbose --debug -o $packfile $GEARS_SO --runcmdargs "$GEARS_OPT" >/tmp/ramp.err 2>&1 || true

	if [[ ! -f /tmp/ramp.fname ]]; then
		eprint "Failed to pack $artifact"
		cat /tmp/ramp.err >&2
		exit 1
	else
		local packname=`cat /tmp/ramp.fname`
	fi

	echo "Created $packname"
}

#----------------------------------------------------------------------------------------------

pack_gearspy() {
	CPYTHON_PREFIX=${CPYTHON_PREFIX:-/var/opt/redislabs/lib/modules/python3}
	if [[ -z $CPYTHON_PREFIX ]]; then
		eprint "$0: CPYTHON_PREFIX is not defined"
		exit 1
	fi
	if [[ -z $GEARSPY_PATH ]]; then
		eprint "$0: GEARSPY_PATH is not defined"
		exit 1
	fi
	if [[ ! -d $CPYTHON_PREFIX ]]; then
		eprint "$0: CPYTHON_PREFIX does not exist"
		exit 1
	fi

	local TAR=artifacts/release/$RELEASE_gearspy
	TAR_PATH=$(realpath $TAR)
	GEARSPY_PATH=$(realpath $GEARSPY_PATH)
	cd $CPYTHON_PREFIX
	# find . -name __pycache__ -type d -exec rm -rf {} \; 2>> /dev/null || true
	export SEMVER
	{ tar -c --sort=name --owner=root:0 --group=root:0 --mtime='UTC 1970-01-01' \
		--transform "s,^./,./python3_$SEMVER/," \
		--transform "s,^.*/gears_python/gears_python\.so,./plugin/gears_python.so," \
		./ $GEARSPY_PATH 2>> /tmp/pack.err | gzip -n - > $TAR_PATH ; E=$?; } || true
	[[ $E != 0 ]] && cat /tmp/pack.err; $(exit $E)
	cd - > /dev/null
	sha256sum $TAR | awk '{print $1}' > $TAR.sha256
	echo "Created $TAR"

	local TAR1=artifacts/snapshot/$SNAPSHOT_gearspy
	cp $TAR $TAR1
	cp $TAR.sha256 $TAR1.sha256
	# ( cd artifacts/snapshot; ln -sf ../../$TAR $TAR1; ln -sf ../../$TAR.sha256 $TAR1.sha256; )
	echo "Created $TAR1"
}

#----------------------------------------------------------------------------------------------

pack_deps() {
	local dep="$1"
	local artdir="$2"
	local package="$3"

	artdir=$(realpath $artdir)
	local depdir=$(cat $artdir/$dep.dir)

	local tar_path=$artdir/$package
	local dep_prefix_dir=$(cat $artdir/$dep.prefix)

	{ cd $depdir ;\
	  cat $artdir/$dep.files | \
	  xargs tar -c --sort=name --owner=root:0 --group=root:0 --mtime='UTC 1970-01-01' \
		--transform "s,^,$dep_prefix_dir," 2>> /tmp/pack.err | \
	  gzip -n - > $tar_path ; E=$?; } || true
	rm -f $artdir/$dep.prefix $artdir/$dep.files $artdir/$dep.dir
	cd $ROOT
	if [[ $E != 0 ]]; then
		eprint "Error creating $tar_path:"
		cat /tmp/pack.err >&2
		exit 1
	fi
	sha256sum $tar_path | awk '{print $1}' > $tar_path.sha256
}

#----------------------------------------------------------------------------------------------

pack_sym() {
	echo "Building debug symbols dependencies ..."
	echo $(dirname $RELEASE_SO) > artifacts/release/debug.dir
	echo $(basename $RELEASE_SO).debug > artifacts/release/debug.files
	echo "" > artifacts/release/debug.prefix
	pack_deps debug artifacts/release $RELEASE_debug

	echo $(dirname $SNAPSHOT_SO) > artifacts/snapshot/debug.dir
	echo $(basename $SNAPSHOT_SO).debug > artifacts/snapshot/debug.files
	echo "" > artifacts/snapshot/debug.prefix
	pack_deps debug artifacts/snapshot $SNAPSHOT_debug
	echo "Done."
}

#----------------------------------------------------------------------------------------------

PACKAGE_NAME=redisgears

[[ -z $BRANCH ]] && BRANCH=${GEARS_PACK_BRANCH:-`git rev-parse --abbrev-ref HEAD`}
BRANCH=${BRANCH//[^A-Za-z0-9._-]/_}
if [[ $GITSHA == 1 ]]; then
	GIT_COMMIT=$(git describe --always --abbrev=7 --dirty="+" 2>/dev/null || git rev-parse --short HEAD)
	BRANCH="${BRANCH}-${GIT_COMMIT}"
fi

NUMVER=$(NUMERIC=1 $ROOT/getver)
SEMVER=$($ROOT/getver)

if [[ ! -z $VARIANT ]]; then
	VARIANT=-${VARIANT}
fi
platform="$OS-$OSNICK-$ARCH"
RELEASE_ramp=${PACKAGE_NAME}.$platform.${SEMVER}${VARIANT}.zip
SNAPSHOT_ramp=${PACKAGE_NAME}.$platform.${BRANCH}${VARIANT}.zip
RELEASE_python_ramp=${PACKAGE_NAME}_python.$platform.${SEMVER}${VARIANT}.zip
SNAPSHOT_python_ramp=${PACKAGE_NAME}_python.$platform.${BRANCH}${VARIANT}.zip
RELEASE_gearspy=${PACKAGE_NAME}-python.$platform.$SEMVER.tgz
SNAPSHOT_gearspy=${PACKAGE_NAME}-python.$platform.$BRANCH.tgz
RELEASE_debug=${PACKAGE_NAME}-debug.$platform.$SEMVER.tgz
SNAPSHOT_debug=${PACKAGE_NAME}-debug.$platform.$BRANCH.tgz
RELEASE_jvm=${PACKAGE_NAME}-jvm.$platform.$SEMVER.tgz
SNAPSHOT_jvm=${PACKAGE_NAME}-jvm.$platform.$BRANCH.tgz

#----------------------------------------------------------------------------------------------

if [[ $JUST_PRINT == 1 ]]; then
	if [[ $RAMP == 1 ]]; then
		[[ $RELEASE == 1 ]] && echo $RELEASE_ramp
		[[ $SNAPSHOT == 1 ]] && echo $SNAPSHOT_ramp
	fi
	if [[ $GEARSPY == 1 ]]; then
		[[ $RELEASE == 1 ]] && echo $RELEASE_gearspy
		[[ $SNAPSHOT == 1 ]] && echo $SNAPSHOT_gearspy
	fi
	exit 0
fi

#----------------------------------------------------------------------------------------------

mkdir -p artifacts/snapshot artifacts/release

if [[ ! -z $MOD ]]; then
	RELEASE_SO=$(realpath $MOD)
	SNAPSHOT_SO=$(dirname $RELEASE_SO)/snapshot/$(basename $RELEASE_SO)
fi

if [[ $RAMP == 1 ]]; then
	if ! command -v redis-server > /dev/null; then
		eprint "$0: Cannot find redis-server. Aborting."
		exit 1
	fi

	[[ -z $MOD ]] && { eprint "$0: Nothing to pack. Aborting."; exit 1; }
	[[ ! -f $MOD ]] && { eprint "$0: $MOD does not exist. Aborting."; exit 1; }
fi

if [[ $SYM == 1 ]]; then
	pack_sym
fi

if [[ $GEARSPY == 1 ]]; then
	pack_gearspy
fi

if [[ $RAMP == 1 ]]; then
	GEARS_SO=$RELEASE_SO GEARSJVM_PKG=artifacts/release/$RELEASE_jvm JAVA_URL_FNAME=$RELEASE_jvm GEARSPY_PKG=artifacts/release/$RELEASE_gearspy URL_FNAME=$RELEASE_gearspy pack release "$RELEASE_ramp"
	GEARS_SO=$SNAPSHOT_SO GEARSJVM_PKG=artifacts/snapshot/$SNAPSHOT_jvm JAVA_URL_FNAME=snapshots/$SNAPSHOT_jvm GEARSPY_PKG=artifacts/snapshot/$SNAPSHOT_gearspy URL_FNAME=snapshots/$SNAPSHOT_gearspy pack snapshot "$SNAPSHOT_ramp"
	GEARS_SO=$RELEASE_SO GEARSJVM_PKG=artifacts/release/$RELEASE_jvm JAVA_URL_FNAME=$RELEASE_jvm GEARSPY_PKG=artifacts/release/$RELEASE_gearspy URL_FNAME=$RELEASE_gearspy pack release "$RELEASE_python_ramp" "true"
	GEARS_SO=$SNAPSHOT_SO GEARSJVM_PKG=artifacts/snapshot/$SNAPSHOT_jvm JAVA_URL_FNAME=snapshots/$SNAPSHOT_jvm GEARSPY_PKG=artifacts/snapshot/$SNAPSHOT_gearspy URL_FNAME=snapshots/$SNAPSHOT_gearspy pack snapshot "$SNAPSHOT_python_ramp" "true"
fi
