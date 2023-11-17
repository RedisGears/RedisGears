OSNICK?=$(shell ../../../deps/readies/bin/platform --osnick)

ifeq ($(DEBUG),1)
	GCC_FLAGS=-o0 -g -DVALGRIND -fno-omit-frame-pointer
else
	GCC_FLAGS=-o2
endif

ifeq ($(GCOV),1)
	GCC_FLAGS+=-fprofile-arcs -ftest-coverage
endif

ifeq ($(OSNICK),catalina)
	GCC_FLAGS+=-undefined dynamic_lookup -DMAC
endif

BASE_DIR=../../../

GCC_FLAGS+=-I $(BASE_DIR)/src/ -I ./

SOURCES=gears_jvm.c $(BASE_DIR)/src/utils/adlist.c
HEADERS=jni.h jni_md.h $(BASE_DIR)/src/redisgears.h $(BASE_DIR)/src/redismodule.h version.h $(BASE_DIR)/src/utils/adlist.h $(BASE_DIR)/src/utils/arr.h $(BASE_DIR)/src/utils/arr_rm_alloc.h

ARTIFACT_NAME=gears_jvm.so

all: $(ARTIFACT_NAME)

$(ARTIFACT_NAME): $(HEADERS) $(SOURCES)
	gcc -std=gnu99 $(GCC_FLAGS) $(SOURCES) -shared -fpic -DREDISMODULE_EXPERIMENTAL_API \
	-o $(ARTIFACT_NAME)

clean:
	rm $(ARTIFACT_NAME)
