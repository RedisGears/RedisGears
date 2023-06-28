#!/bin/bash
set -x
set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
JVM_PLUGIN_ROOT=$(cd $HERE/../ && pwd)

if [ -z $3 ]; then
    echo "Usage: ${0} /path/to/gears_python_installation /path/to/gearspython.so /path/to/redisgears.so <option a... option n>"
    exit 3
fi

OS="$(../../../deps/readies/bin/platform --os)"

if [[ $OS == macos ]]; then
    JAVA_BIN=${JVM_PLUGIN_ROOT}/bin/OpenJDK/jdk-17.0.7+7/Contents/Home/bin/
    JVM_PATH=${JVM_PLUGIN_ROOT}/bin/OpenJDK/jdk-17.0.7+7/Contents/Home/
else
    JAVA_BIN=${JVM_PLUGIN_ROOT}/bin/OpenJDK/jdk-17.0.7+7/bin/
    JVM_PATH=${JVM_PLUGIN_ROOT}/bin/OpenJDK/jdk-17.0.7+7/
fi

mkdir -p gears_tests/build
cd gears_tests/build
${JAVA_BIN}/javac -d ./ -classpath ${JVM_PLUGIN_ROOT}/gears_runtime/target/gear_runtime-jar-with-dependencies.jar ../src/gears_tests/*
${JAVA_BIN}/jar -cvf gears_tests.jar ./gears_tests/
cd ../..

JVM_OPTIONS="-Djava.class.path="
JVM_OPTIONS+="${JVM_PLUGIN_ROOT}/gears_runtime/target/gear_runtime-jar-with-dependencies.jar"

PYTHONDIR=$1
GEARSPYTHON=$2
GEARSLIB=$3
GEARSJVM=../../src/gears_jvm.so
shift 3

python3 -m RLTest --module ${GEARSLIB} --module-args "Plugin ${GEARSJVM} JvmPath $JVM_PATH JvmOptions $JVM_OPTIONS Plugin ${GEARSPYTHON} CreateVenv 0 PythonInstallationDir ${PYTHONDIR}" --clear-logs "$@"

echo cluster 1 shard
python3 -m RLTest --module ${GEARSLIB} --module-args "Plugin ${GEARSJVM} JvmPath $JVM_PATH JvmOptions $JVM_OPTIONS Plugin ${GEARSPYTHON} CreateVenv 0 PythonInstallationDir ${PYTHONDIR}" --clear-logs --env oss-cluster --shards-count 1 "$@"

echo cluster 2 shards
python3 -m RLTest --module ${GEARSLIB} --module-args "Plugin ${GEARSJVM} JvmPath $JVM_PATH JvmOptions $JVM_OPTIONS Plugin ${GEARSPYTHON} CreateVenv 0 PythonInstallationDir ${PYTHONDIR}" --clear-logs --env oss-cluster --shards-count 2 "$@"

echo cluster 3 shards
python3 -m RLTest --module ${GEARSLIB} --module-args "Plugin ${GEARSJVM} JvmPath $JVM_PATH JvmOptions $JVM_OPTIONS Plugin ${GEARSPYTHON} CreateVenv 0 PythonInstallationDir ${PYTHONDIR}" --clear-logs --env oss-cluster --shards-count 3 "$@"

rm -rf ../bin/RedisGears/.venv-*
