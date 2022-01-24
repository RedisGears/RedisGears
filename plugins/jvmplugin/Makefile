OS?=$(shell uname -s)
ARCH?=$(shell uname -m)
OSNICK?=$(shell ../../deps/readies/bin/platform --osnick)
GIT_BRANCH=$(shell ../../getbranch)
VERSION=$(shell ../../getver)

ifndef PYTHONDIR
$(error Specify the path to python as PYTHONDIR)
endif

ifeq ($(shell test -e ../../gears_python.so && echo yes),)
$(error Build redisgears first)
endif

# in case you want to specifcy
GEARSPYTHONLIB=$(shell readlink -f ../../gears_python.so)
GEARSLIB=$(shell readlink -f ../../redisgears.so)

$(info OS=$(OS))

all: gears_jvm GearsRuntime pack

.PHONY: InstallOpenJDK

gears_jvm: InstallOpenJDK
	make -C ./src/

/tmp/openjdk-hotspot.zip:
	test -f /tmp/opendjk-hotspot.zip || wget -q "https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.9.1%2B1/OpenJDK11U-jdk_x64_linux_hotspot_11.0.9.1_1.tar.gz" -O /tmp/openjdk-hotspot.zip

InstallOpenJDK: bin/OpenJDK

bin/OpenJDK: /tmp/openjdk-hotspot.zip
	test -d ./bin/OpenJDK || mkdir -p ./bin/OpenJDK
	tar -C ./bin/OpenJDK -xvf /tmp/openjdk-hotspot.zip

GearsRuntime:
	cd gears_runtime; mvn -q package

clean:
	make -C ./src/ clean

tests: gears_jvm
	cd ./pytest; ./run_test.sh ${PYTHONDIR} ${GEARSPYTHONLIB} ${GEARSLIB} --parallelism `nproc`

run: gears_jvm
	redis-server --loadmodule ${GEARSLIB} Plugin ./src/gears_jvm.so JvmOptions "-Djava.class.path=./gears_runtime/target/gear_runtime-jar-with-dependencies.jar" JvmPath ./bin/OpenJDK/jdk-11.0.9.1+1/

run_valgrind:
	valgrind --leak-check=full --log-file=output.val redis-server --loadmodule ${GEARSLIB} Plugin ./src/gears_jvm.so JvmOptions "-Djava.class.path=./gears_runtime/target/gear_runtime-jar-with-dependencies.jar" JvmPath ./bin/OpenJDK/jdk-11.0.9.1+1/

pack: gears_jvm
	OSNICK=$(OSNICK) GIT_BRANCH=$(GIT_BRANCH) VERSION=$(VERSION) ./pack.sh
