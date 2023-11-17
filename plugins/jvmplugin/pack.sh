#!/bin/bash

set -e

# consistency - lifted from pack.sh, until we rewrite
[[ $ARCH == x64 ]] && ARCH=x86_64
[[ $OS == linux ]] && OS=Linux

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

if [[ $OS == macos ]]; then
    JVM_PATH=./bin/OpenJDK/jdk-17.0.7+7/Contents/Home/
else
    JVM_PATH=./bin/OpenJDK/jdk-17.0.7+7/
fi

BRANCH_PLUGIN_TAR=../../artifacts/snapshot/redisgears-jvm.${OS}-${OSNICK}-${ARCH}.$GIT_BRANCH.tgz
tar -czf $BRANCH_PLUGIN_TAR \
	--transform "s,^./src/,./plugin/," \
	$JVM_PATH/* \
	./src/gears_jvm.so \
	./gears_runtime/target/gear_runtime-jar-with-dependencies.jar
sha256sum $BRANCH_PLUGIN_TAR | cut -d ' ' -f 1-1 > $BRANCH_PLUGIN_TAR.sha256

VERSION_PLUGIN_TAR=../../artifacts/release/redisgears-jvm.${OS}-${OSNICK}-${ARCH}.$VERSION.tgz
tar -czf $VERSION_PLUGIN_TAR \
	--transform "s,^./src/,./plugin/," \
	$JVM_PATH/* \
	./src/gears_jvm.so \
	./gears_runtime/target/gear_runtime-jar-with-dependencies.jar
sha256sum $VERSION_PLUGIN_TAR | cut -d ' ' -f 1-1 > $VERSION_PLUGIN_TAR.sha256
