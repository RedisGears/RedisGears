ifeq ($(DEBUG),1)
	GCC_FLAGS=-o0 -g -DVALGRIND -fno-omit-frame-pointer
else
	GCC_FLAGS=-o2
endif

ifeq ($(GCOV),1)
	GCC_FLAGS+=-fprofile-arcs -ftest-coverage
endif

GCC_FLAGS+=-I ../../../src/ -I ./

SOURCES=gears_jvm.c ../../../src/utils/adlist.c
HEADERS=jni.h jni_md.h ../../../src/redisgears.h ../../../src/redismodule.h version.h ../../../src/utils/adlist.h ../../../src/utils/arr.h ../../../src/utils/arr_rm_alloc.h

ARTIFACT_NAME=gears_jvm.so

all: $(ARTIFACT_NAME)

$(ARTIFACT_NAME): $(HEADERS) $(SOURCES)
	gcc -std=gnu99 $(GCC_FLAGS) $(SOURCES) -shared -fpic -DREDISMODULE_EXPERIMENTAL_API \
	-o $(ARTIFACT_NAME)

clean:
	rm $(ARTIFACT_NAME)
