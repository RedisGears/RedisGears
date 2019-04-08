
ROOT=.

.NOTPARALLEL:

ifeq ($(SHOW),1)
override SHOW:=
else
override SHOW:=@
endif

MAKEFLAGS += --no-builtin-rules  --no-print-directory
# --no-builtin-variables

DEPENDENCIES=cpython libevent

define __SEP
import os; rows, cols = os.popen('stty size', 'r').read().split(); print(\"\n\" + '-' * (int(cols) - 1) + \"\n\")
endef

#----------------------------------------------------------------------------------------------

OS=linux
# OS := $(shell sh -c 'uname -s 2>/dev/null || echo not')
ARCH=x64

ifeq ($(DEBUG),1)
FLAVOR=debug
else
FLAVOR=release
endif

GIT_SHA := $(shell git rev-parse HEAD)

__VARIANT__=$(shell if [ -f VARIANT ]; then cat VARIANT; fi)

ifeq ($(origin VARIANT),undefined)
ifneq ($(__VARIANT__),)
VARIANT:=$(__VARIANT__)
endif
endif

ifeq ($(VARIANT),)
__VARIANT:=
else
__VARIANT:=-$(VARIANT)
endif
FULL_VARIANT:=$(OS)-$(ARCH)-$(FLAVOR)$(__VARIANT)
FULL_VARIANT_1:=$(OS)-$(ARCH)-release$(__VARIANT)

# BLAND_VARIANT:=$(OS)-$(ARCH)-$(FLAVOR)$(__VARIANT)
# BLAND_VARIANT:=$(OS)-$(ARCH)$(__VARIANT)
# PURE_VARIANT:=$(OS)-$(ARCH)-$(FLAVOR)

ifneq ($(origin VARIANT),)
$(eval $(shell if [ -z $(VARIANT) ]; then rm -f VARIANT; else echo $(VARIANT)>VARIANT; fi))
endif

ifneq ($(filter all deps cpython libevent,$(MAKECMDGOALS)),)
DEPS=1
endif

#----------------------------------------------------------------------------------------------

BINROOT=bin/$(FULL_VARIANT)
BINDIR=$(BINROOT)/$(SRCDIR)
BIN_DIRS=$(sort $(patsubst %/,%,$(BINDIR) $(dir $(OBJECTS))))

define mkdir_rule
$(1):
	$$(SHOW)mkdir -p $(1)
endef

#----------------------------------------------------------------------------------------------

WITHPYTHON ?= 1

export PYTHON_ENCODING ?= ucs2

LIBPYTHON=bin/$(FULL_VARIANT_1)/cpython/libpython2.7.a

#----------------------------------------------------------------------------------------------

LIBEVENT=bin/$(FULL_VARIANT_1)/libevent/.libs/libevent.a

#----------------------------------------------------------------------------------------------

CC=gcc
SRCDIR := src

export CPYTHON_PREFIX=/opt/redislabs/lib/modules/python27

_SOURCES=utils/adlist.c utils/buffer.c utils/dict.c module.c execution_plan.c \
	mgmt.c keys_reader.c keys_writer.c example.c filters.c mappers.c \
	extractors.c reducers.c record.c cluster.c commands.c streams_reader.c \
	globals.c config.c lock_handler.c module_init.c
ifeq ($(WITHPYTHON),1)
_SOURCES += redisgears_python.c
endif

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst %.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst %.c, $(BINROOT)/%.d, $(SOURCES))

CC_FLAGS=\
	-fPIC -std=gnu99 \
	-MMD -MF $(@:.o=.d) \
	-include $(SRCDIR)/common.h \
	-I$(SRCDIR) -Iinclude -I$(BINDIR) \
	-DREDISGEARS_GIT_SHA=\"$(GIT_SHA)\" -DCPYTHON_PATH=\"$(CPYTHON_PREFIX)/\" \
	-DREDISMODULE_EXPERIMENTAL_API

TARGET=$(BINROOT)/redisgears.so

ifeq ($(DEBUG),1)
CC_FLAGS += -g -O0 -DVALGRIND
LD_FLAGS += -g
else
CC_FLAGS += -O2 -Wno-unused-result
endif

ifeq ($(WITHPYTHON), 1)
CPYTHON_DIR=deps/cpython

CC_FLAGS += \
	-DWITHPYTHON \
	-I$(CPYTHON_DIR)/Include \
	-I$(CPYTHON_DIR) \
	-I$(BINROOT)/cpython \
	-Ibin/$(FULL_VARIANT_1)/cpython

LD_FLAGS += 
EMBEDDED_LIBS += $(LIBPYTHON) -lutil
endif # WITHPYTHON

OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

EMBEDDED_LIBS += $(LIBEVENT)

#----------------------------------------------------------------------------------------------

.PHONY: all __sep bindir deps cpython libevent pyenv build static clean pack ramp_pack test

build: bindirs $(TARGET)

all: __sep bindirs deps build pack

__sep: ;
#	@python -c "$(__SEP)"

#----------------------------------------------------------------------------------------------

bindirs: $(BIN_DIRS)

$(foreach DIR,$(BIN_DIRS),$(eval $(call mkdir_rule,$(DIR))))

#----------------------------------------------------------------------------------------------

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $^...
	$(SHOW)$(CC) -I$(SRCDIR) -Ideps $(CC_FLAGS) -c $< -o $@

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

$(TARGET:.so=.a): $(OBJECTS) $(LIBEVENT)
	@echo Creating $@...
	$(SHOW)$(AR) rcs $@ $(filter-out module_init,$(OBJECTS)) $(LIBEVENT)

#----------------------------------------------------------------------------------------------

get_deps:
	$(SHOW)git submodule update --init --recursive
	$(SHOW)$(MAKE) --no-print-directory -C build/libevent source

ifeq ($(DEPS),1)

#----------------------------------------------------------------------------------------------

deps: $(LIBPYTHON) $(LIBEVENT)

cpython: $(LIBPYTHON)

$(LIBPYTHON):
	@echo Building cpython...
	$(SHOW)$(MAKE) --no-print-directory -C build/cpython MAKEFLAGS= SHOW=$(SHOW) VARIANT=$(VARIANT)
#PYTHON_ENCODING=$(PYTHON_ENCODING)

pyenv:
	@echo Building pyenv...
	$(SHOW)$(MAKE) --no-print-directory -C build/cpython pyenv 
#MAKEFLAGS=

#----------------------------------------------------------------------------------------------

libevent: $(LIBEVENT)

$(LIBEVENT):
	@echo Building libevent...
	$(SHOW)$(MAKE) --no-print-directory -C build/libevent MAKEFLAGS= SHOW=$(SHOW) VARIANT=$(VARIANT)

#----------------------------------------------------------------------------------------------

endif # DEPS

#----------------------------------------------------------------------------------------------

clean:
	$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	$(SHOW)rm -f $(TARGET) $(TARGET:.so=.a) $(notdir $(TARGET)) artifacts/release/* artifacts/snapshot/*
ifeq ($(DEPS),1) 
	$(SHOW)$(foreach DEP,$(DEPENDENCIES),$(MAKE) --no-print-directory -C build/$(DEP) clean MAKEFLAGS= SHOW=$(SHOW) VARIANT=$(VARIANT);)
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

