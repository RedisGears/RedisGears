
ROOT=.
include build/Makefile.defs

BINDIR=$(BINROOT)/$(SRCDIR)

#----------------------------------------------------------------------------------------------

DEPENDENCIES=cpython libevent

ifneq ($(filter all deps $(DEPENDENCIES),$(MAKECMDGOALS)),)
DEPS=1
endif

#----------------------------------------------------------------------------------------------

WITHPYTHON ?= 1

ifeq ($(WITHPYTHON),1)
export PYTHON_ENCODING ?= ucs2

# LIBPYTHON=bin/$(FULL_VARIANT_REL)/cpython/libpython2.7.a

CPYTHON_BINDIR=bin/$(FULL_VARIANT_REL)/cpython

include build/cpython/Makefile.defs
endif # WITHPYTHON

#----------------------------------------------------------------------------------------------

# LIBEVENT=bin/$(FULL_VARIANT_REL)/libevent/.libs/libevent.a

LIBEVENT_BINDIR=bin/$(FULL_VARIANT_REL)/libevent

include build/libevent/Makefile.defs

#----------------------------------------------------------------------------------------------

CC=gcc
SRCDIR=src

_SOURCES=utils/adlist.c utils/buffer.c utils/dict.c module.c execution_plan.c \
	mgmt.c keys_reader.c keys_writer.c example.c filters.c mappers.c \
	extractors.c reducers.c record.c cluster.c commands.c streams_reader.c \
	globals.c config.c lock_handler.c module_init.c
ifeq ($(WITHPYTHON),1)
_SOURCES += redisgears_python.c
endif

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
# OBJECTS=$(patsubst %.c,$(BINDIR)/%.o,$(SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

# CC_DEPS = $(patsubst %.c, $(BINROOT)/%.d, $(SOURCES))
CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES))

CC_FLAGS += \
	-fPIC -std=gnu99 \
	-MMD -MF $(@:.o=.d) \
	-include $(SRCDIR)/common.h \
	-I$(SRCDIR) -Iinclude -I$(BINDIR) -Ideps \
	-DREDISGEARS_GIT_SHA=\"$(GIT_SHA)\" \
	-DREDISMODULE_EXPERIMENTAL_API

TARGET=$(BINROOT)/redisgears.so

ifeq ($(DEBUG),1)
CC_FLAGS += -g -O0 -DVALGRIND
LD_FLAGS += -g
else
CC_FLAGS += -O2 -Wno-unused-result
endif

#----------------------------------------------------------------------------------------------

ifeq ($(WITHPYTHON), 1)

CPYTHON_DIR=deps/cpython

CC_FLAGS += \
	-DWITHPYTHON \
	-DCPYTHON_PATH=\"$(CPYTHON_PREFIX)/\" \
	-I$(CPYTHON_DIR)/Include \
	-I$(CPYTHON_DIR) \
	-I$(BINROOT)/cpython \
	-Ibin/$(FULL_VARIANT_REL)/cpython

LD_FLAGS += 
EMBEDDED_LIBS += $(LIBPYTHON) -lutil

endif # WITHPYTHON

EMBEDDED_LIBS += $(LIBEVENT)

#----------------------------------------------------------------------------------------------

define HELP

Building RedisGears from scratch:

make setup # install packages required for build
make fetch # download and prepare dependant modules (i.e., python, libevent)
make all   # build everything
make test  # run tests


endef

#----------------------------------------------------------------------------------------------

.NOTPARALLEL:

.PHONY: all deps $(DEPENDENCIES) pyenv build static clean pack ramp_pack test

build: bindirs $(TARGET)

all: deps build pack

include build/Makefile.rules

#----------------------------------------------------------------------------------------------

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $^...
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@

$(SRCDIR)/redisgears_python.c : $(BINDIR)/GearsBuilder.auto.h $(BINDIR)/cloudpickle.auto.h

$(BINDIR)/GearsBuilder.auto.h: $(SRCDIR)/GearsBuilder.py
	$(SHOW)xxd -i $< > $@

$(BINDIR)/cloudpickle.auto.h: $(SRCDIR)/cloudpickle.py
	$(SHOW)xxd -i $< > $@

#----------------------------------------------------------------------------------------------

ifeq ($(DEPS),1)
$(TARGET): $(OBJECTS) $(LIBEVENT) $(LIBPYTHON)
else
$(TARGET): $(OBJECTS)
endif
	@echo Linking $@...
	$(SHOW)$(CC) -shared -o $@ $(OBJECTS) $(LD_FLAGS) -Wl,--whole-archive $(EMBEDDED_LIBS) -Wl,--no-whole-archive
	$(SHOW)ln -sf $(TARGET) $(notdir $(TARGET))

static: $(TARGET:.so=.a)

$(TARGET:.so=.a): $(OBJECTS) $(LIBEVENT) $(LIBPYTHON)
	@echo Creating $@...
	$(SHOW)$(AR) rcs $@ $(filter-out module_init,$(OBJECTS)) $(LIBEVENT)

#----------------------------------------------------------------------------------------------

setup:
	@echo Setting up system...
	$(SHOW)./system-setup.sh

get_deps fetch:
	$(SHOW)git submodule update --init --recursive
	$(SHOW)$(MAKE) --no-print-directory -C build/libevent source

#----------------------------------------------------------------------------------------------

ifeq ($(DEPS),1)

#----------------------------------------------------------------------------------------------

deps: $(LIBPYTHON) $(LIBEVENT)

cpython: $(LIBPYTHON)

$(LIBPYTHON):
	@echo Building cpython...
	$(SHOW)$(MAKE) --no-print-directory -C build/cpython MAKEFLAGS= SHOW=$(SHOW) DEBUG= VARIANT=$(VARIANT)
#PYTHON_ENCODING=$(PYTHON_ENCODING)

pyenv:
	@echo Building pyenv...
	$(SHOW)$(MAKE) --no-print-directory -C build/cpython pyenv 
#MAKEFLAGS=

#----------------------------------------------------------------------------------------------

libevent: $(LIBEVENT)

$(LIBEVENT):
	@echo Building libevent...
	$(SHOW)$(MAKE) --no-print-directory -C build/libevent MAKEFLAGS= SHOW=$(SHOW) DEBUG= VARIANT=$(VARIANT)

#----------------------------------------------------------------------------------------------

endif # DEPS

#----------------------------------------------------------------------------------------------

clean:
	-$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	$(SHOW)rm -f $(TARGET) $(TARGET:.so=.a) $(notdir $(TARGET)) artifacts/release/* artifacts/snapshot/*
ifeq ($(DEPS),1) 
	$(SHOW)$(foreach DEP,$(DEPENDENCIES),$(MAKE) --no-print-directory -C build/$(DEP) clean MAKEFLAGS= ALL=$(ALL) SHOW=$(SHOW) VARIANT=$(VARIANT);)
endif

#----------------------------------------------------------------------------------------------

pack ramp_pack: __sep deps build $(CPYTHON_PREFIX)
	$(SHOW)./pack.sh $(TARGET)

#----------------------------------------------------------------------------------------------

test: __sep
ifeq ($(DEBUG),1)
	$(SHOW)cd pytest; ./run_tests_valgrind.sh
else
	$(SHOW)cd pytest; ./run_tests.sh
endif

#----------------------------------------------------------------------------------------------
