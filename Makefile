
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
make hiredis    # build hiredis
make cpython    # build cpython
make all        # build all libraries and packages

make test          # run tests
  VGD=1            # run tests with Valgrind
  TEST=test        # run specific `test` with Python debugger
  TEST_ARGS=args   # additional RLTest arguments
  GDB=1            # (with TEST=...) run with GDB

make pack          # build packages (ramp & dependencies)
make ramp_pack     # only build ramp package
make verify-packs  # verify signatures of packages vs module so

make show-version  # show module version
endef

MK_ALL_TARGETS=bindirs deps build ramp_pack verify-packs

include $(MK)/defs

GEARS_VERSION:=$(shell $(ROOT)/getver)
OS_VERSION_DESC:=$(shell python $(ROOT)/getos.py)

#----------------------------------------------------------------------------------------------

DEPENDENCIES=cpython libevent hiredis

ifneq ($(filter all deps $(DEPENDENCIES) pack ramp_pack,$(MAKECMDGOALS)),)
DEPS=1
endif

#----------------------------------------------------------------------------------------------

WITHPYTHON ?= 1

ifeq ($(WITHPYTHON),1)

export PYTHON_ENCODING ?= ucs4

CPYTHON_BINDIR=bin/$(FULL_VARIANT.release)/cpython
CPYTHON_BINROOT=bin/$(FULL_VARIANT.release)

include build/cpython/Makefile.defs

endif # WITHPYTHON

#----------------------------------------------------------------------------------------------

LIBEVENT_BINDIR=bin/$(FULL_VARIANT.release)/libevent

include build/libevent/Makefile.defs

#----------------------------------------------------------------------------------------------

HIREDIS_BINDIR=bin/$(FULL_VARIANT.release)/hiredis

include build/hiredis/Makefile.defs

#----------------------------------------------------------------------------------------------

CC=gcc
SRCDIR=src

_SOURCES=utils/adlist.c utils/buffer.c utils/dict.c module.c execution_plan.c \
	mgmt.c readers/keys_reader.c example.c filters.c mappers.c utils/thpool.c \
	extractors.c reducers.c record.c cluster.c commands.c readers/streams_reader.c \
	globals.c config.c lock_handler.c module_init.c slots_table.c common.c readers/command_reader.c \
	readers/shardid_reader.c crc16.c
ifeq ($(WITHPYTHON),1)
_SOURCES += redisgears_python.c
endif
ifeq ($(DEBUG),1)
_SOURCES += debug.c
endif

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES))

CC_FLAGS += \
	-fPIC -std=gnu99 \
	-MMD -MF $(@:.o=.d) \
	-include $(SRCDIR)/common.h \
	-I$(SRCDIR) \
	-I$(BINDIR) \
	-Ideps \
	-I. \
	-I deps/libevent/include \
	-Ibin/$(FULL_VARIANT.release)/libevent/include \
	-Ideps/hiredis \
	-Ideps/hiredis/adapters \
	-DREDISGEARS_GIT_SHA=\"$(GIT_SHA)\" \
	-DREDISGEARS_OS_VERSION=\"$(OS_VERSION_DESC)\" \
	-DREDISMODULE_EXPERIMENTAL_API

TARGET=$(BINROOT)/redisgears.so
TARGET.snapshot=$(BINROOT)/snapshot/redisgears.so

ifeq ($(DEBUG),1)
CC_FLAGS += -g -O0 -DVALGRIND
LD_FLAGS += -g
else
CC_FLAGS += -O2 -Wno-unused-result
endif

ifeq ($(OS),macosx)
LD_FLAGS += \
	-framework CoreFoundation \
	-undefined dynamic_lookup
endif

#----------------------------------------------------------------------------------------------
# cpython-related definitions
#----------------------------------------------------------------------------------------------

ifeq ($(WITHPYTHON), 1)

CPYTHON_DIR=deps/cpython

CC_FLAGS += \
	-DWITHPYTHON \
	-DCPYTHON_PATH=\"$(abspath $(CPYTHON_BINROOT))/\" \
	-I$(CPYTHON_DIR)/Include \
	-I$(CPYTHON_DIR) \
	-I$(BINROOT)/cpython \
	-Ibin/$(FULL_VARIANT.release)/cpython

LD_FLAGS +=

EMBEDDED_LIBS += $(LIBPYTHON)

ifeq ($(wildcard $(LIBPYTHON)),)
MISSING_DEPS += $(LIBPYTHON)
endif

ifeq ($(OS),macosx)
LD_FLAGS += \
	$(GETTEXT_PREFIX)/lib/libintl.a \
	-liconv
else
EMBEDDED_LIBS += -lutil -luuid
endif

endif # WITHPYTHON

#----------------------------------------------------------------------------------------------

ifeq ($(SHOW_LD_LIBS),1)
LD_FLAGS += -Wl,-t
endif

ifeq ($(OS),macosx)
EMBEDDED_LIBS += $(LIBEVENT) $(HIREDIS)
endif

ifeq ($(wildcard $(LIBEVENT)),)
MISSING_DEPS += $(LIBEVENT)
endif

ifeq ($(wildcard $(HIREDIS)),)
MISSING_DEPS += $(HIREDIS)
endif

#----------------------------------------------------------------------------------------------

ifneq ($(MISSING_DEPS),)
DEPS=1
endif

#----------------------------------------------------------------------------------------------

.NOTPARALLEL:

MK_CUSTOM_CLEAN=1

.PHONY: deps $(DEPENDENCIES) static pack ramp_pack test setup fetch

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

RAMP_VARIANT=$(subst release,,$(FLAVOR))$(_VARIANT.string)

RAMP.release:=$(shell JUST_PRINT=1 RAMP=1 DEPS=0 RELEASE=1 SNAPSHOT=0 VARIANT=$(RAMP_VARIANT) ./pack.sh)
RAMP.snapshot:=$(shell JUST_PRINT=1 RAMP=1 DEPS=0 RELEASE=0 SNAPSHOT=1 VARIANT=$(RAMP_VARIANT) ./pack.sh)
DEPS_TAR.release:=$(shell JUST_PRINT=1 RAMP=0 DEPS=1 RELEASE=1 SNAPSHOT=0 ./pack.sh)
DEPS_TAR.snapshot:=$(shell JUST_PRINT=1 RAMP=0 DEPS=1 RELEASE=0 SNAPSHOT=1 ./pack.sh)

#----------------------------------------------------------------------------------------------

DEPS_URL_BASE:=http://redismodules.s3.amazonaws.com/redisgears
DEPS_URL_BASE.release=$(DEPS_URL_BASE)
DEPS_URL_BASE.snapshot=$(DEPS_URL_BASE)/snapshots

define build_deps_args # type (release|snapshot)
$$(BINDIR)/$(1)-deps.o : $$(SRCDIR)/deps-args.c artifacts/$(1)/$(DEPS_TAR.$(1))
	$$(SHOW)$$(CC) $$(CC_FLAGS) -c $$< -o $$@ \
		-DDEPENDENCIES_URL=\"$$(DEPS_URL_BASE.$(1))/$$(DEPS_TAR.$(1))\" \
		-DDEPENDENCIES_SHA256=\"$$(shell cat artifacts/$(1)/$$(DEPS_TAR.$(1)).sha256)\"
endef

$(eval $(call build_deps_args,release))
$(eval $(call build_deps_args,snapshot))

#----------------------------------------------------------------------------------------------

ifeq ($(OS),macosx)
EMBEDDED_LIBS_FLAGS=$(foreach L,$(EMBEDDED_LIBS),-Wl,-force_load,$(L))
else
EMBEDDED_LIBS_FLAGS=\
	-Wl,-Bstatic $(HIREDIS) $(LIBEVENT) -Wl,-Bdynamic \
	-Wl,--whole-archive $(EMBEDDED_LIBS) -Wl,--no-whole-archive
endif

STRIP:=strip --strip-debug --strip-unneeded

$(TARGET): $(MISSING_DEPS) $(OBJECTS) $(BINDIR)/release-deps.o $(BINDIR)/snapshot-deps.o
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

$(TARGET:.so=.a): $(OBJECTS) $(LIBEVENT) $(LIBPYTHON) $(HIREDIS)
	@echo Creating $@...
	$(SHOW)$(AR) rcs $@ $(filter-out module_init,$(OBJECTS)) $(LIBEVENT) $(HIREDIS)

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

deps: $(LIBPYTHON) $(LIBEVENT) $(HIREDIS)

cpython: $(LIBPYTHON)

$(LIBPYTHON):
	@echo Building cpython...
	$(SHOW)$(MAKE) --no-print-directory -C build/cpython DEBUG=

#----------------------------------------------------------------------------------------------

libevent: $(LIBEVENT)

$(LIBEVENT):
	@echo Building libevent...
	$(SHOW)$(MAKE) --no-print-directory -C build/libevent DEBUG=

#----------------------------------------------------------------------------------------------

hiredis: $(HIREDIS)

$(HIREDIS):
	@echo Building hiredis...
	$(SHOW)$(MAKE) --no-print-directory -C build/hiredis DEBUG=

#----------------------------------------------------------------------------------------------

else

deps: ;

endif # DEPS

#----------------------------------------------------------------------------------------------

ifeq ($(DIAG),1)
$(info *** MK_CLEAN_DIR=$(MK_CLEAN_DIR))
$(info *** LIBPYTHON=$(LIBPYTHON))
$(info *** LIBEVENT=$(LIBEVENT))
$(info *** HIREDIS=$(HIREDIS))
$(info *** MISSING_DEPS=$(MISSING_DEPS))
endif

clean:
ifeq ($(ALL),1)
	$(SHOW)rm -rf $(BINDIR) $(TARGET) $(TARGET.snapshot) $(notdir $(TARGET)) $(BINROOT)/redislabs
else
	-$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	$(SHOW)rm -f $(TARGET) $(TARGET.snapshot) $(TARGET:.so=.a) $(notdir $(TARGET)) \
		artifacts/release/$(DEPS_TAR.release)* artifacts/snapshot/$(DEPS_TAR.snapshot)*
endif
ifeq ($(DEPS),1)
	$(SHOW)$(foreach DEP,$(DEPENDENCIES),$(MAKE) --no-print-directory -C build/$(DEP) clean;)
endif

#----------------------------------------------------------------------------------------------

artifacts/release/$(RAMP.release) artifacts/snapshot/$(RAMP.snapshot): $(TARGET) ramp.yml
	@echo Packing module...
	$(SHOW)RAMP=1 DEPS=0 VARIANT=$(RAMP_VARIANT) ./pack.sh $(TARGET)

artifacts/release/$(DEPS_TAR.release) artifacts/snapshot/$(DEPS_TAR.snapshot): $(CPYTHON_PREFIX)
	@echo Packing dependencies...
	$(SHOW)RAMP=0 DEPS=1 CPYTHON_PREFIX=$(CPYTHON_PREFIX) ./pack.sh $(TARGET)

ramp_pack: artifacts/release/$(RAMP.release) artifacts/snapshot/$(RAMP.snapshot)

pack: artifacts/release/$(RAMP.release) artifacts/snapshot/$(RAMP.snapshot) \
		artifacts/release/$(DEPS_TAR.release) artifacts/snapshot/$(DEPS_TAR.snapshot)

verify-packs:
	@set -e ;\
	MOD=`./deps/readies/bin/redis-cmd --loadmodule $(TARGET) -- RG.CONFIGGET dependenciesSha256 2> /tmp/redisgears-sha.err` ;\
	if [[ -z $MOD ]]; then \
		>2& cat /tmp/redisgears-sha.err ;\
		exit 1 ;\
	fi ;\
	REL=`cat artifacts/release/$(DEPS_TAR.release).sha256` ;\
	SNAP=`cat artifacts/snapshot/$(DEPS_TAR.snapshot).sha256` ;\
	if [[ $$MOD != $$REL || $$REL != $$SNAP ]]; then \
		echo "Module and package SHA256 don't match." ;\
		echo "\"$$MOD\"" ;\
		echo "\"$$REL\"" ;\
		echo "\"$$SNAP\"" ;\
		exit 1 ;\
	else \
		echo "Signatures match." ;\
	fi

#----------------------------------------------------------------------------------------------

show-version:
	$(SHOW)echo $(GEARS_VERSION)

#----------------------------------------------------------------------------------------------

ifeq ($(GDB),1)
RLTEST_GDB=-i
endif

ifeq ($(VGD),1)
TEST_FLAGS += VALGRIND=1
endif

test: __sep
ifneq ($(TEST),)
	@set -e; cd pytest; BB=1 $(TEST_FLAGS) RLTest --test $(TEST) $(TEST_ARGS) $(RLTEST_GDB) -s --module $(abspath $(ROOT)/redisgears.so)
else
	$(SHOW)set -e; cd pytest; $(TEST_FLAGS) ./run_tests.sh
endif

#----------------------------------------------------------------------------------------------
