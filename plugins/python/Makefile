ifeq ($(DEBUG),1)
	GCC_FLAGS=-o0 -g -DVALGRIND -fno-omit-frame-pointer
else
	GCC_FLAGS=-o2
endif

REDISGEARS_SRC=../../src

CPYTHON_DIR=../../deps/cpython

GCC_FLAGS += \
	-DCPYTHON_PATH=\"$(CPYTHON_PATH)\" \
	-I$(CPYTHON_DIR)/Include \
	-I$(CPYTHON_DIR) \
	-I$(BINROOT)/cpython \
	-Ibin/$(FULL_VARIANT.release)/cpython \
	-I$(REDISGEARS_SRC)/ \
	-I$(REDISGEARS_SRC)/utils/

SOURCES=redisgears_python.c globals.c $(REDISGEARS_SRC)/utils/dict.c $(REDISGEARS_SRC)/utils/thpool.c \
$(REDISGEARS_SRC)/utils/buffer.c
HEADERS=GearsBuilder.auto.h cloudpickle.auto.h globals.h redisai.h $(REDISGEARS_SRC)/redisgears.h \
$(REDISGEARS_SRC)/utils/dict.h $(REDISGEARS_SRC)/utils/thpool.h $(REDISGEARS_SRC)/utils/buffer.h

ARTIFACT_NAME=gears_python.so

all: $(ARTIFACT_NAME)

GearsBuilder.auto.h: GearsBuilder.py
	$(SHOW)xxd -i $< > $@

cloudpickle.auto.h: cloudpickle.py
	$(SHOW)xxd -i $< > $@

$(ARTIFACT_NAME): $(HEADERS) $(SOURCES)
	gcc -std=gnu99 $(GCC_FLAGS) $(SOURCES) -shared -fpic -DREDISMODULE_EXPERIMENTAL_API \
	../../bin/linux-x64-release/cpython/libpython3.7m.a \
	-o $(ARTIFACT_NAME)

clean:
	rm $(ARTIFACT_NAME)