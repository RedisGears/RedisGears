OPEN_J9_DIR=../../deps/openj9-openjdk-jdk14
OPEN_J9_BUILD_DIR=$(OPEN_J9_DIR)/build/linux-x86_64-server-release

ifeq ($(DEBUG),1)
	GCC_FLAGS=-o0 -g -DVALGRIND -fno-omit-frame-pointer
else
	GCC_FLAGS=-o2
endif

all:
	gcc $(GCC_FLAGS) gears_jvm.c -shared -fpic -DREDISMODULE_EXPERIMENTAL_API \
	-I$(OPEN_J9_BUILD_DIR)/jdk/include \
	-I$(OPEN_J9_BUILD_DIR)/jdk/include/linux \
	-L$(OPEN_J9_BUILD_DIR)/jdk/lib/server \
	-ljvm \
	-o gears_jvm.so
