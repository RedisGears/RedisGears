
PLATFORM:=$(shell python -c 'import platform; print "".join(platform.dist()[0:2])')

MAKEFLAGS += --no-builtin-rules

WITH_PYTHON ?= 1

CC=gcc
SRCDIR := src
BINDIR := obj
GIT_SHA := $(shell git rev-parse HEAD)

SOURCES=utils/adlist.c utils/buffer.c utils/dict.c module.c execution_plan.c \
	mgmt.c keys_reader.c keys_writer.c example.c filters.c mappers.c \
	extractors.c reducers.c record.c cluster.c commands.c streams_reader.c \
	globals.c config.c lock_handler.c module_init.c

OBJECTS=$(patsubst %.c, $(BINDIR)/%.o, $(SOURCES))

CFLAGS=\
	-fPIC -std=gnu99 \
	-I$(SRCDIR) -Iinclude \
	-DREDISMODULE_EXPERIMENTAL_API -DREDISGEARS_GIT_SHA=\"$(GIT_SHA)\"

ifeq ($(DEBUG),1)
CFLAGS += -g -O0 -DVALGRIND
else
CFLAGS += -O2 -Wno-unused-result
endif

ifeq ($(WITH_PYTHON),1)
CPYTHON_DIR=$(SRCDIR)/deps/cpython
SOURCES += redisgears_python.c
CFLAGS += -I$(CPYTHON_DIR)/Include -Ibuild/cpython -DWITHPYTHON
LDFLAGS +=
EMBEDDED_LIBS += $(LIBPYTHON) -lutil
endif

LIBPYTHON=build/cpython/libpython2.7.a
LIBEVENT=build/libevent/.libs/libevent.a

EMBEDDED_LIBS += $(LIBEVENT)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $^...
	$(CC) -I$(SRCDIR) $(CFLAGS) -c $< -o $@

ifeq ($(ALL),1)
all: $(BINDIR) python libevent redisgears.so
else
all: $(BINDIR) redisgears.so
endif

$(BINDIR):
	@mkdir -p $(BINDIR) $(BINDIR)/utils

$(LIBPYTHON):
	@echo Building cpython...
	@$(MAKE) -C build/cpython -f Makefile.main

python: $(LIBPYTHON)

python_clean:
	@$(MAKE) -C build/cpython -f Makefile.main clean

libevent: $(LIBEVENT)

$(LIBEVENT):
	@echo Building libevent...
	@$(MAKE) -C build/libevent -f Makefile.main

redisgears.so: $(OBJECTS) $(LIBEVENT) $(LIBPYTHON)
	@echo Linking $@...
	$(CC) -shared -o $@ $(OBJECTS) $(LDFLAGS) -Wl,--whole-archive $(EMBEDDED_LIBS) -Wl,--no-whole-archive 

redisgears.a: $(OBJECTS)
	$(AR) rcs $@ $(filter-out module_init,$(OBJECTS)) $(LIBEVENT)

static: redisgears.a

clean:
	find obj -name '*.[oad]' -type f -delete
	rm -f redisgears.so redisgears.a redisgears.zip
ifeq ($(ALL),1) 
	@$(MAKE) -C build/cpython -f Makefile.main clean
	@$(MAKE) -C build/libevent -f Makefile.main clean
endif
	
deps: python libevent

get_deps: deps

ramp_pack: all
	@ramp pack $(realpath ./redisgears.so) -m ramp.yml -o redisgears.zip
