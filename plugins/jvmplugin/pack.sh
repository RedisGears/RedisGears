#!/bin/bash

set -e

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT=$(cd $HERE/../.. && pwd)
export READIES=$ROOT/deps/readies
. $READIES/shibumi/defs

# consistency - lifted from pack.sh, until we rewrite
[[ $OSNICK == trusty ]]  && OSNICK=ubuntu14.04
[[ $OSNICK == xenial ]]  && OSNICK=ubuntu16.04
[[ $OSNICK == bionic ]]  && OSNICK=ubuntu18.04
[[ $OSNICK == focal ]]   && OSNICK=ubuntu20.04
[[ $OSNICK == centos7 ]] && OSNICK=rhel7
[[ $OSNICK == centos8 ]] && OSNICK=rhel8
[[ $OSNICK == ol8 ]] && OSNICK=rhel8
[[ $OSNICK == rocky8 ]] && OSNICK=rhel8

if [[ $OSNICK == catalina ]]; then
    JVM_PATH=./bin/OpenJDK/jdk-11.0.14+9/Contents/Home/
else
    JVM_PATH=./bin/OpenJDK/jdk-11.0.9.1+1/
fi

ARTDIR=$ROOT/bin/artifacts

tar -czf $ARTDIR/snapshot/redisgears-jvm.Linux-$OSNICK-x86_64.$GIT_BRANCH.tgz \
--transform "s,^./src/,./plugin/," \
$JVM_PATH/* \
./src/gears_jvm.so \
./gears_runtime/target/gear_runtime-jar-with-dependencies.jar
sha256sum $ARTDIR/snapshot/redisgears-jvm.Linux-$OSNICK-x86_64.$GIT_BRANCH.tgz |cut -d ' ' -f 1-1 > $ARTDIR/snapshot/redisgears-jvm.Linux-$OSNICK-x86_64.$GIT_BRANCH.tgz.sha256

tar -czf $ARTDIR/release/redisgears-jvm.Linux-$OSNICK-x86_64.$VERSION.tgz \
--transform "s,^./src/,./plugin/," \
$JVM_PATH/* \
./src/gears_jvm.so \
./gears_runtime/target/gear_runtime-jar-with-dependencies.jar
sha256sum $ARTDIR/release/redisgears-jvm.Linux-$OSNICK-x86_64.$VERSION.tgz | cut -d ' ' -f 1-1 > $ARTDIR/release/redisgears-jvm.Linux-$OSNICK-x86_64.$VERSION.tgz.sha256
