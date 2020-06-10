#!/bin/bash

JVM_OPTIONS="-Djava.class.path="
JVM_OPTIONS+="./gears_runtime/target/gear_runtime-0.0.2-SNAPSHOT-jar-with-dependencies.jar"
#JVM_OPTIONS+=" -XX:+IdleTuningGcOnIdle";
JVM_OPTIONS+=" -Xms10m";
JVM_OPTIONS+=" -Xmx2048m";
JVM_OPTIONS+=" -Xrs";
#JVM_OPTIONS+=" -Xcheck:jni";

echo $JVM_OPTIONS

#LD_LIBRARY_PATH=/home/meir/work/RedisGears/deps/openj9-openjdk-jdk14/build/linux-x86_64-server-release/jdk/lib/server/ redis-server --loadmodule ../../redisgears.so PluginsDirectory ./ JvmOptions "$JVM_OPTIONS"
#LD_LIBRARY_PATH=/usr/lib/jvm/java-11-openjdk-amd64/lib/server/ redis-server --loadmodule ../../redisgears.so PluginsDirectory ./ JvmOptions "$JVM_OPTIONS"
LD_LIBRARY_PATH=../../deps/openj9-openjdk-jdk14/build/linux-x86_64-server-release/jdk/lib/server/ redis-server --loadmodule ../../redisgears.so PluginsDirectory ./ JvmOptions "$JVM_OPTIONS"
