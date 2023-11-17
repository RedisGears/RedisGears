
OS ?= $(shell ../../deps/readies/bin/platform --os)
ARCH ?= $(shell ../../deps/readies/bin/platform --arch)
OSNICK ?= $(shell ../../deps/readies/bin/platform --osnick)
ifeq ($(OSNICK),bigsur)
override OSNICK:=catalina
endif

GIT_BRANCH:=$(shell ../../getbranch)
VERSION:=$(shell ../../getver)

ifndef NOGEARS

ifndef PYTHONDIR
$(error Specify the path to python as PYTHONDIR)
endif

ifeq ($(shell test -e ../../gears_python.so && echo yes),)
$(error Build redisgears first)
endif

endif # NOGEARS

ifeq ($(OS),macos)
	JVM_DOWNLOAD_LINK=https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.7%2B7/OpenJDK17U-jdk_x64_mac_hotspot_17.0.7_7.tar.gz
	JDK_VER=17.0.7+7
	PARALLELISM=1
	READLINK=greadlink
else
	JVM_DOWNLOAD_LINK=https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.7%2B7/OpenJDK17U-jdk_x64_linux_hotspot_17.0.7_7.tar.gz
	JDK_VER=17.0.7+7
	PARALLELISM=4
	READLINK=readlink
endif

# in case you want to specifcy
GEARSPYTHONLIB:=$(shell ${READLINK} -f ../../gears_python.so)
GEARSLIB:=$(shell ${READLINK} -f ../../redisgears.so)

$(info OS=$(OS))

all: gears_jvm GearsRuntime pack

.PHONY: InstallOpenJDK

gears_jvm: InstallOpenJDK
	make -C src

/tmp/openjdk-hotspot.zip:
	test -f /tmp/openjdk-hotspot.zip || wget -q ${JVM_DOWNLOAD_LINK} -O /tmp/openjdk-hotspot.zip

InstallOpenJDK: bin/OpenJDK

bin/OpenJDK: /tmp/openjdk-hotspot.zip
	test -d bin/OpenJDK || mkdir -p bin/OpenJDK
	tar -C bin/OpenJDK -xf /tmp/openjdk-hotspot.zip

GearsRuntime:
	cd gears_runtime; mvn -q package

clean:
	make -C src clean

tests: gears_jvm
	cd ./pytest ;\
	./run_test.sh ${PYTHONDIR} ${GEARSPYTHONLIB} ${GEARSLIB} --parallelism ${PARALLELISM}

run: gears_jvm
	redis-server --loadmodule ${GEARSLIB} Plugin ./src/gears_jvm.so JvmOptions "-Djava.class.path=./gears_runtime/target/gear_runtime-jar-with-dependencies.jar" JvmPath ./bin/OpenJDK/jdk-$(JDK_VER)/

debug_run: gears_jvm
	redis-server --loadmodule ${GEARSLIB} Plugin ./src/gears_jvm.so JvmOptions "-Djava.class.path=./gears_runtime/target/gear_runtime-jar-with-dependencies.jar -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8000" JvmPath ./bin/OpenJDK/jdk-$(JDK_VER)/

run_cluster: gears_jvm
	RLTest -s --env oss-cluster --shards-count 2 --module ../../redisgears.so --module-args "Plugin ../src/gears_jvm.so JvmOptions -Djava.class.path=../gears_runtime/target/gear_runtime-jar-with-dependencies.jar JvmPath ../bin/OpenJDK/jdk-$(JDK_VER)/" --env-only -s --clear-logs

run_valgrind:
	valgrind --leak-check=full --log-file=output.val redis-server --loadmodule ${GEARSLIB} Plugin ./src/gears_jvm.so JvmOptions "-Djava.class.path=./gears_runtime/target/gear_runtime-jar-with-dependencies.jar" JvmPath ./bin/OpenJDK/jdk-$(JDK_VER)/

pack: gears_jvm
	OSNICK=$(OSNICK) OS=$(OS) ARCH=$(ARCH) GIT_BRANCH=$(GIT_BRANCH) VERSION=$(VERSION) ./pack.sh

publish:
	cd gears_runtime; mvn -s .circleci.settings.xml -DskipTests deploy
