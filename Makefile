
SHOW=@
PLATFORM:=$(shell python -c 'import platform; print "".join(platform.dist()[0:2])')

MAKEFLAGS += --no-builtin-rules --no-builtin-variables

WITH_PYTHON ?= 1

#----------------------------------------------------------------------------------------------

.NOTPARALLEL:
 
CC=gcc
SRCDIR := src
BINDIR := obj
GIT_SHA := $(shell git rev-parse HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
OS := $(shell lsb_release -si)

_SOURCES=utils/adlist.c utils/buffer.c utils/dict.c module.c execution_plan.c \
	mgmt.c keys_reader.c keys_writer.c example.c filters.c mappers.c \
	extractors.c reducers.c record.c cluster.c commands.c streams_reader.c \
	globals.c config.c lock_handler.c module_init.c

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst %.c, $(BINDIR)/%.o, $(SOURCES))

CFLAGS=\
	-fPIC -std=gnu99 \
	-include $(SRCDIR)/common.h \
	-I$(SRCDIR) -Iinclude \
	-DREDISMODULE_EXPERIMENTAL_API -DREDISGEARS_GIT_SHA=\"$(GIT_SHA)\"

ifeq ($(DEBUG),1)
CFLAGS += -g -O0 -DVALGRIND
LDFLAGS += -g
else
CFLAGS += -O2 -Wno-unused-result
endif

ifeq ($(WITH_PYTHON),1)
CPYTHON_DIR=$(SRCDIR)/deps/cpython
SOURCES += src/redisgears_python.c
CFLAGS += -I$(CPYTHON_DIR)/Include -Ibuild/cpython -DWITHPYTHON
LDFLAGS +=
EMBEDDED_LIBS += $(LIBPYTHON) -lutil
endif

LIBPYTHON=build/cpython/libpython2.7.a
LIBEVENT=build/libevent/.libs/libevent.a

EMBEDDED_LIBS += $(LIBEVENT)

all: bindirs python libevent redisgears.so

#----------------------------------------------------------------------------------------------

BIN_DIRS=$(sort $(patsubst %/,%,$(BINDIR) $(dir $(OBJECTS))))

bindirs: $(BIN_DIRS)

define mkdir_rule
$(1):
	$$(SHOW)mkdir -p $(1)
endef

$(foreach DIR,$(BIN_DIRS),$(eval $(call mkdir_rule,$(DIR))))

#----------------------------------------------------------------------------------------------

$(BINDIR)/%.o: %.c
	@echo Compiling $^...
	$(CC) -I$(SRCDIR) -Ideps $(CFLAGS) -c $< -o $@

#----------------------------------------------------------------------------------------------

python: $(LIBPYTHON)

$(LIBPYTHON):
	@echo Building cpython...
	@$(MAKE) -C build/cpython -f Makefile.main MAKEFLAGS=

python_clean:
	@$(MAKE) -C build/cpython -f Makefile.main clean MAKEFLAGS=

#----------------------------------------------------------------------------------------------

libevent: $(LIBEVENT)

$(LIBEVENT):
	@echo Building libevent...
	@$(MAKE) -C build/libevent -f Makefile.main

#----------------------------------------------------------------------------------------------

redisgears.so: $(OBJECTS) $(LIBEVENT) $(LIBPYTHON)
	@echo Linking $@...
	$(CC) -shared -o $@ $(OBJECTS) $(LDFLAGS) -Wl,--whole-archive $(EMBEDDED_LIBS) -Wl,--no-whole-archive 

redisgears.a: $(OBJECTS)
	$(AR) rcs $@ $(filter-out module_init,$(OBJECTS)) $(LIBEVENT)

static: redisgears.a

#----------------------------------------------------------------------------------------------

clean:
	find obj -name '*.[oad]' -type f -delete
	rm -f redisgears.so redisgears.a redisgears.zip artifacts/*.zip
ifeq ($(DEPS),1) 
	@$(MAKE) -C build/cpython -f Makefile.main clean
	@$(MAKE) -C build/libevent -f Makefile.main clean
endif
	
#----------------------------------------------------------------------------------------------

deps: python libevent

get_deps: deps

ramp_pack: all
	mkdir artifacts; \
	ramp pack $(realpath ./redisgears.so) -m ramp.yml -o artifacts/redisgears-$(GIT_BRANCH)-$(OS)-{architecture}.{semantic_version}.zip
