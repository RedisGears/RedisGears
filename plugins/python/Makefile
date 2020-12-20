ifeq ($(DEBUG),1)
	GCC_FLAGS=-o0 -g -DVALGRIND -fno-omit-frame-pointer
else
	GCC_FLAGS=-o2
endif

ROOTDIR=../../

REDISGEARS_SRC=$(ROOTDIR)/src

CPYTHON_DIR=$(ROOTDIR)/deps/cpython
CPYTHON_BUILD_DIR=$(ROOTDIR)/bin/linux-x64-release/cpython/

GCC_FLAGS += \
	-I$(CPYTHON_DIR)/Include \
	-I$(CPYTHON_DIR) \
	-I$(CPYTHON_BUILD_DIR) \
	-I$(REDISGEARS_SRC)/ \
	-I$(REDISGEARS_SRC)/utils/

ifneq ($(CPYTHON_PATH),)
	GCC_FLAGS += -DCPYTHON_PATH=\"$(CPYTHON_PATH)\"
endif

ifneq ($(BINROOT),)
	BINDIR=$(BINROOT)/plugins/python/
else
	BINDIR=./
endif

SOURCES=redisgears_python.c globals.c $(REDISGEARS_SRC)/utils/dict.c $(REDISGEARS_SRC)/utils/thpool.c \
$(REDISGEARS_SRC)/utils/buffer.c
HEADERS=GearsBuilder.auto.h cloudpickle.auto.h globals.h redisai.h $(REDISGEARS_SRC)/redisgears.h \
$(REDISGEARS_SRC)/utils/dict.h $(REDISGEARS_SRC)/utils/thpool.h $(REDISGEARS_SRC)/utils/buffer.h


ARTIFACT_FILE_NAME=gears_python.so
ARTIFACT_NAME=$(BINDIR)$(ARTIFACT_FILE_NAME)

all: $(ARTIFACT_NAME)

GearsBuilder.auto.h: GearsBuilder.py
	xxd -i $< > $@

cloudpickle.auto.h: cloudpickle.py
	xxd -i $< > $@

$(ARTIFACT_NAME): $(HEADERS) $(SOURCES)
	mkdir -p $(BINDIR)
	gcc -std=gnu99 $(GCC_FLAGS) $(SOURCES) -shared -fpic -DREDISMODULE_EXPERIMENTAL_API \
	../../bin/linux-x64-release/cpython/libpython3.7m.a \
	-o $(ARTIFACT_NAME)
	ln -sf $(ARTIFACT_NAME) $(ROOTDIR)$(ARTIFACT_FILE_NAME)

clean:
	rm $(ARTIFACT_NAME) $(ROOTDIR)$(ARTIFACT_FILE_NAME) GearsBuilder.auto.h cloudpickle.auto.h