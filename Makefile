
ROOT=.
include deps/readies/mk/main

BINDIR=$(BINROOT)/$(SRCDIR)

#----------------------------------------------------------------------------------------------

define HELP
make setup      # install packages required for build
make fetch      # download and prepare dependant modules (i.e., python, libevent)

make build
  DEBUG=1       # build debug variant
  VARIANT=name
  WITHPYTHON=0  # build without embedded Python interpreter
  DEPS=1        # also build dependant modules
make clean      # remove binary files
  ALL=1         # remove binary directories
  DEPS=1        # also clean dependant modules

make deps       # build dependant modules
make libevent   # build libevent
make cpython    # build cpython
make pyenv      # install cpython and virtual environment
make all        # build all libraries and packages

make test       # run tests
make pack       # build packages (ramp & dependencies)
endef

MK_ALL_TARGETS=bindirs deps pyenv build pack

include $(MK)/defs

#----------------------------------------------------------------------------------------------

DEPENDENCIES=cpython libevent

ifneq ($(filter all deps $(DEPENDENCIES) pyenv pack ramp_pack,$(MAKECMDGOALS)),)
DEPS=1
endif

#----------------------------------------------------------------------------------------------

WITHPYTHON ?= 1

ifeq ($(WITHPYTHON),1)
export PYTHON_ENCODING ?= ucs4

CPYTHON_BINDIR=bin/$(FULL_VARIANT.release)/cpython

include build/cpython/Makefile.defs
endif # WITHPYTHON

#----------------------------------------------------------------------------------------------

LIBEVENT_BINDIR=bin/$(FULL_VARIANT.release)/libevent

include build/libevent/Makefile.defs

#----------------------------------------------------------------------------------------------

CC=gcc
SRCDIR=src

_SOURCES=utils/adlist.c utils/buffer.c utils/dict.c module.c execution_plan.c \
	mgmt.c keys_reader.c example.c filters.c mappers.c \
	extractors.c reducers.c record.c cluster.c commands.c streams_reader.c \
	globals.c config.c lock_handler.c module_init.c slots_table.c common.c
ifeq ($(WITHPYTHON),1)
_SOURCES += redisgears_python.c
endif

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

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

ifeq ($(OS),macosx)
LD_FLAGS += -undefined dynamic_lookup
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
	-Ibin/$(FULL_VARIANT.release)/cpython

LD_FLAGS += 

EMBEDDED_LIBS += $(LIBPYTHON)

ifeq ($(wildcard $(LIBPYTHON)),)
MISSING_DEPS += $(LIBPYTHON)
endif

ifneq ($(OS),macosx)
EMBEDDED_LIBS += -lutil
endif

endif # WITHPYTHON

EMBEDDED_LIBS += $(LIBEVENT)

ifeq ($(wildcard $(LIBEVENT)),)
MISSING_DEPS += $(LIBEVENT)
endif

#----------------------------------------------------------------------------------------------

ifneq ($(MISSING_DEPS),)
DEPS=1
endif

#----------------------------------------------------------------------------------------------

.NOTPARALLEL:

MK_CUSTOM_CLEAN=1

.PHONY: deps $(DEPENDENCIES) pyenv static pack ramp_pack test setup fetch

# build: bindirs $(TARGET)

include $(MK)/rules

#----------------------------------------------------------------------------------------------

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@

$(SRCDIR)/redisgears_python.c : $(BINDIR)/GearsBuilder.auto.h $(BINDIR)/cloudpickle.auto.h

$(BINDIR)/GearsBuilder.auto.h: $(SRCDIR)/GearsBuilder.py
	$(SHOW)xxd -i $< > $@

$(BINDIR)/cloudpickle.auto.h: $(SRCDIR)/cloudpickle.py
	$(SHOW)xxd -i $< > $@

#----------------------------------------------------------------------------------------------

DEPS_TAR.release:=$(shell JUST_PRINT=1 PACK_RAMP=0 PACK_DEPS=1 RELEASE=1 SNAPSHOT=0 ./pack.sh)
DEPS_TAR.snapshot:=$(shell JUST_PRINT=1 PACK_RAMP=0 PACK_DEPS=1 RELEASE=0 SNAPSHOT=1 ./pack.sh)

artifacts/release/$(DEPS_TAR.release) artifacts/snapshot/$(DEPS_TAR.snapshot): $(CPYTHON_PREFIX)
	@echo Packing dependencies ...
	$(SHOW)PACK_DEPS=1 PACK_RAMP=0 RELEASE=1 SNAPSHOT=1 ./pack.sh $(TARGET)
	$(SHOW)sha256sum artifacts/release/$(DEPS_TAR.release) | gawk '{print $$1}' > artifacts/release/$(DEPS_TAR.release).sha256
	$(SHOW)sha256sum artifacts/snapshot/$(DEPS_TAR.snapshot) | gawk '{print $$1}' > artifacts/snapshot/$(DEPS_TAR.snapshot).sha256

$(BINDIR)/release-deps.o : $(SRCDIR)/deps-args.c artifacts/release/$(DEPS_TAR.release)
	@echo "release-build: $(DEPS_TAR.release)"
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@ \
		-DDEPENDENCIES_URL=\"http://redismodules.s3.amazonaws.com/redisgears/$(DEPS_TAR.release)\" \
		-DDEPENDENCIES_SHA256=\"$(shell cat artifacts/release/$(DEPS_TAR.release).sha256)\"

$(BINDIR)/snapshot-deps.o : $(SRCDIR)/deps-args.c artifacts/snapshot/$(DEPS_TAR.snapshot)
	@echo "snapshot-build: $(DEPS_TAR.snapshot)"
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@ \
		-DDEPENDENCIES_URL=\"http://redismodules.s3.amazonaws.com/redisgears/snapshot/$(DEPS_TAR.snapshot)\" \
		-DDEPENDENCIES_SHA256=\"$(shell cat artifacts/snapshot/$(DEPS_TAR.snapshot).sha256)\"

#----------------------------------------------------------------------------------------------

ifeq ($(OS),macosx)
EMBEDDED_LIBS_FLAGS=$(foreach L,$(EMBEDDED_LIBS),-Wl,-force_load,$(L))
else
EMBEDDED_LIBS_FLAGS=-Wl,--whole-archive $(EMBEDDED_LIBS) -Wl,--no-whole-archive
endif

STRIP:=strip --strip-debug --strip-unneeded

$(TARGET): $(MISSING_DEPS) $(OBJECTS) $(BINDIR)/release-deps.o artifacts/snapshot/$(DEPS_TAR.release) $(BINDIR)/snapshot-deps.o artifacts/snapshot/$(DEPS_TAR.snapshot)
	@echo Linking $@...
	$(SHOW)$(CC) -shared -o $@ $(OBJECTS) $(BINDIR)/release-deps.o $(LD_FLAGS) $(EMBEDDED_LIBS_FLAGS)
	$(SHOW)mkdir -p $(dir $@)/snapshot
	$(SHOW)$(CC) -shared -o $(dir $@)/snapshot/$(notdir $@) $(OBJECTS) $(BINDIR)/snapshot-deps.o $(LD_FLAGS) $(EMBEDDED_LIBS_FLAGS)
ifneq ($(DEBUG),1)
ifneq ($(OS),macosx)
	$(SHOW)$(STRIP) $@
	$(SHOW)$(STRIP) $(dir $@)/snapshot/$(notdir $@)
endif
endif
	$(SHOW)ln -sf $(TARGET) $(notdir $(TARGET))

static: $(TARGET:.so=.a)

$(TARGET:.so=.a): $(OBJECTS) $(LIBEVENT) $(LIBPYTHON)
	@echo Creating $@...
	$(SHOW)$(AR) rcs $@ $(filter-out module_init,$(OBJECTS)) $(LIBEVENT)

#----------------------------------------------------------------------------------------------

# we avoid $(SUDO) here since we expect 'sudo make setup'

setup:
	@echo Setting up system...
	$(SHOW)./deps/readies/bin/getpy2
	$(SHOW)./system-setup.py

fetch get_deps:
	-$(SHOW)git submodule update --init --recursive
	$(SHOW)$(MAKE) --no-print-directory -C build/libevent source

#----------------------------------------------------------------------------------------------

ifeq ($(DEPS),1)

#----------------------------------------------------------------------------------------------

deps: $(LIBPYTHON) $(LIBEVENT)

cpython: $(LIBPYTHON)

$(LIBPYTHON):
	@echo Building cpython...
	$(SHOW)$(MAKE) --no-print-directory -C build/cpython DEBUG=

pyenv:
	@echo Building pyenv...
	$(SHOW)$(MAKE) --no-print-directory -C build/cpython DEBUG= pyenv

#----------------------------------------------------------------------------------------------

libevent: $(LIBEVENT)

$(LIBEVENT):
	@echo Building libevent...
	$(SHOW)$(MAKE) --no-print-directory -C build/libevent DEBUG=

#----------------------------------------------------------------------------------------------

else

deps: ;

endif # DEPS

#----------------------------------------------------------------------------------------------

ifeq ($(DIAG),1)
$(info *** MK_CLEAN_DIR=$(MK_CLEAN_DIR))
$(info *** LIBPYTHON=$(LIBPYTHON))
$(info *** LIBEVENT=$(LIBEVENT))
$(info *** MISSING_DEPS=$(MISSING_DEPS))
endif

clean:
ifeq ($(ALL),1)
	$(SHOW)rm -rf $(BINDIR) $(TARGET) $(notdir $(TARGET)) $(BINROOT)/redislabs
else
	-$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	$(SHOW)rm -f $(TARGET) $(TARGET:.so=.a) $(notdir $(TARGET)) artifacts/release/* artifacts/snapshot/*
endif
ifeq ($(DEPS),1)
	$(SHOW)$(foreach DEP,$(DEPENDENCIES),$(MAKE) --no-print-directory -C build/$(DEP) clean;)
endif

#----------------------------------------------------------------------------------------------

pack ramp_pack: __sep deps build $(CPYTHON_PREFIX)
	$(SHOW)./pack.sh $(TARGET)

#----------------------------------------------------------------------------------------------

ifeq ($(GDB),1)
RLTEST_GDB=-i
endif

test: __sep
ifeq ($(DEBUG),1)
	$(SHOW)set -e; cd pytest; ./run_tests_valgrind.sh
else
ifneq ($(TEST),)
	@set -e; cd pytest; PYDEBUG=1 RLTest --test $(TEST) $(RLTEST_GDB) -s --module $(abspath $(TARGET))
else
	$(SHOW)set -e; cd pytest; ./run_tests.sh
endif
endif

#----------------------------------------------------------------------------------------------
