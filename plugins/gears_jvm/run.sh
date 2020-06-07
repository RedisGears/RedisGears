#!/bin/bash

JVM_OPTIONS="-Djava.class.path="
JVM_OPTIONS+="./gears_runtime/bin/:"
JVM_OPTIONS+="./gears_runtime/lib/jackson-annotations-2.11.0.jar:"
JVM_OPTIONS+="./gears_runtime/lib/jackson-core-2.11.0.jar:"
JVM_OPTIONS+="./gears_runtime/lib/jackson-databind-2.11.0.jar"
#JVM_OPTIONS+=" -XX:+IdleTuningGcOnIdle"
JVM_OPTIONS+=" -Xms10m"
JVM_OPTIONS+=" -Xmx2048m"
#JVM_OPTIONS+=" -Xint"
#JVM_OPTIONS+=" -Xcheck:jni";

echo $JVM_OPTIONS

LD_LIBRARY_PATH=../../deps/openj9-openjdk-jdk14/build/linux-x86_64-server-release/jdk/lib/server/ redis-server --loadmodule ../../redisgears.so PluginsDirectory ./ JvmOptions "$JVM_OPTIONS"
#LD_LIBRARY_PATH=/usr/lib/jvm/java-11-openjdk-amd64/lib/server/ redis-server --loadmodule ../../redisgears.so PluginsDirectory ./ JvmOptions "$JVM_OPTIONS"
#LD_LIBRARY_PATH=../../deps/openj9-openjdk-jdk14/build/linux-x86_64-server-release/vm/ redis-server --loadmodule ../../redisgears.so PluginsDirectory ./ JvmOptions "$JVM_OPTIONS"
