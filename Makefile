
GCC=1

WITH_PYTHON ?= 1

ifeq ($(VG),1)
override VALGRIND:=1
endif

ifeq ($(VALGRIND),1)
override DEBUG:=1
endif

ROOT=.
include deps/readies/mk/main

BINDIR=$(BINROOT)/$(SRCDIR)

#----------------------------------------------------------------------------------------------

define HELPTEXT

make build
  DEBUG=1         # build debug variant
  VG|VALGRIND=1   # build with VALGRIND (implies DEBUG=1)
  VARIANT=name    # build as variant `name`
  WITH_PYTHON=0   # build without embedded Python interpreter
  DEPS=1          # also build dependant modules
make clean        # remove binary files
  ALL=1           # remove binary directories
  DEPS=1          # also clean dependant modules

make deps          # build dependant modules
make libevent      # build libevent
make hiredis       # build hiredis
make gears_python  # build Gears Python plugin
make all           # build all libraries and packages

make test          # run tests
  VG|VALGRIND=1    # run tests with Valgrind
  TEST=test        # run specific `test` with Python debugger
  TEST_ARGS=args   # additional RLTest arguments
  GDB=1            # (with TEST=...) run with GDB

make pack          # build packages (ramp & python plugin)
make ramp-pack     # only build ramp package
make verify-packs  # verify signatures of packages vs module so

make show-version  # show module version

make platform      # build for given platform
  OSNICK=nick      # OS/distribution to build for
  ARTIFACTS=1      # copy artifacts from builder container
  TEST=1           # run tests
endef

MK_ALL_TARGETS=bindirs deps build ramp-pack verify-packs

include $(MK)/defs

GEARS_VERSION:=$(shell $(ROOT)/getver)
OS_VERSION_DESC:=$(shell python3 $(ROOT)/getos.py)

#----------------------------------------------------------------------------------------------

DEPENDENCIES=libevent hiredis
ifeq ($(WITH_PYTHON),1)
DEPENDENCIES += gears_python
endif

ifneq ($(filter all deps $(DEPENDENCIES) pack ramp-pack,$(MAKECMDGOALS)),)
DEPS=1
endif

#----------------------------------------------------------------------------------------------

ifeq ($(WITH_PYTHON),1)
include plugins/python/Makefile.defs
endif

#----------------------------------------------------------------------------------------------

LIBEVENT_BINDIR=bin/$(FULL_VARIANT.release)/libevent
include build/libevent/Makefile.defs

#----------------------------------------------------------------------------------------------

HIREDIS_BINDIR=bin/$(FULL_VARIANT.release)/hiredis
include build/hiredis/Makefile.defs

#----------------------------------------------------------------------------------------------

SRCDIR=src

define _SOURCES:=
	cluster.c
	commands.c
	common.c
	config.c
	crc16.c
	execution_plan.c
	global.c
	lock_handler.c
	mappers.c
	mgmt.c
	module.c
	module_init.c
	command_hook.c
	readers/command_reader.c
	readers/keys_reader.c
	readers/shardid_reader.c
	readers/streams_reader.c
	record.c
	slots_table.c
	utils/adlist.c
	utils/buffer.c
	utils/dict.c
	utils/thpool.c
endef

ifeq ($(DEBUG),1)
_SOURCES += debug.c
endif

SOURCES=$(addprefix $(SRCDIR)/,$(filter %,$(subst $(__NL), ,$(_SOURCES))))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES))

define _CC_FLAGS
	-fPIC
	-std=gnu99
	-MMD
	-MF $(@:.o=.d)
	-I$(SRCDIR)
	-I$(BINDIR)
	-Ideps
	-I.
	-Ideps/libevent/include
	-Ibin/$(FULL_VARIANT.release)/libevent/include
	-Ideps/hiredis
	-Ideps/hiredis/adapters

	-DREDISGEARS_GIT_SHA=\"$(GIT_SHA)\"
	-DREDISGEARS_OS_VERSION=\"$(OS_VERSION_DESC)\"
	-DREDISMODULE_EXPERIMENTAL_API
endef
CC_FLAGS += $(filter %,$(subst $(__NL), ,$(_CC_FLAGS)))

TARGET=$(BINROOT)/redisgears.so
TARGET.snapshot=$(BINROOT)/snapshot/redisgears.so

ifeq ($(VALGRIND),1)
CC_FLAGS += -DVALGRIND
endif

ifeq ($(DEBUG),1)
CC_FLAGS += -g -O0
LD_FLAGS += -g
else
CC_FLAGS += -O3 -Wno-unused-result
endif

ifeq ($(GCOV),1)
CC_FLAGS += -fprofile-arcs -ftest-coverage
LD_FLAGS += -fprofile-arcs -ftest-coverage
endif

ifeq ($(OS),macos)
CC_FLAGS += -I/usr/local/opt/openssl/include/
LD_FLAGS += \
	-framework CoreFoundation \
	-undefined dynamic_lookup
endif

#----------------------------------------------------------------------------------------------

ifeq ($(SHOW_LD_LIBS),1)
LD_FLAGS += -Wl,-t
endif

ifeq ($(OS),macos)
EMBEDDED_LIBS += $(LIBEVENT) $(HIREDIS)
endif

MISSING_DEPS:=
ifeq ($(wildcard $(LIBEVENT)),)
MISSING_DEPS += $(LIBEVENT)
endif

ifeq ($(wildcard $(HIREDIS)),)
MISSING_DEPS += $(HIREDIS)
endif

ifeq ($(wildcard $(GEARS_PLUGIN)),)
MISSING_DEPS += $(GEARS_PLUGIN)
endif

#----------------------------------------------------------------------------------------------

ifneq ($(MISSING_DEPS),)
DEPS=1
endif

#----------------------------------------------------------------------------------------------

.NOTPARALLEL:

MK_CUSTOM_CLEAN=1

.PHONY: deps $(DEPENDENCIES) static pack ramp ramp-pack test setup verify-packs platform jvmplugin

include $(MK)/rules

#----------------------------------------------------------------------------------------------

-include $(CC_DEPS)

$(BINDIR)/global.o: $(SRCDIR)/global.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -c -include $(SRCDIR)/common.h $< -o $@

#----------------------------------------------------------------------------------------------

RAMP_VARIANT=$(subst release,,$(FLAVOR))$(_VARIANT.string)

RAMP.release:=$(shell JUST_PRINT=1 RAMP=1 RELEASE=1 SNAPSHOT=0 VARIANT=$(RAMP_VARIANT) ./pack.sh)
RAMP.snapshot:=$(shell JUST_PRINT=1 RAMP=1 RELEASE=0 SNAPSHOT=1 VARIANT=$(RAMP_VARIANT) ./pack.sh)
GEARS_PYTHON_TAR.release:=$(shell JUST_PRINT=1 RAMP=0 GEARSPY=1 RELEASE=1 SNAPSHOT=0 ./pack.sh)
GEARS_PYTHON_TAR.snapshot:=$(shell JUST_PRINT=1 RAMP=0 GEARSPY=1 RELEASE=0 SNAPSHOT=1 ./pack.sh)

#----------------------------------------------------------------------------------------------

DEPS_URL_BASE:=http://redismodules.s3.amazonaws.com/redisgears
DEPS_URL_BASE.release=$(DEPS_URL_BASE)
DEPS_URL_BASE.snapshot=$(DEPS_URL_BASE)/snapshots

define build_deps_args # type (release|snapshot)
$$(BINDIR)/$(1)-deps.o : $$(SRCDIR)/deps-args.c artifacts/$(1)/$(GEARS_PYTHON_TAR.$(1))
	$$(SHOW)$$(CC) $$(CC_FLAGS) -c $$< -o $$@ \
		-DGEARS_PYTHON_URL=\"$$(DEPS_URL_BASE.$(1))/$$(GEARS_PYTHON_TAR.$(1))\" \
		-DGEARS_PYTHON_SHA256=\"$$(shell cat artifacts/$(1)/$$(GEARS_PYTHON_TAR.$(1)).sha256)\"
endef

$(eval $(call build_deps_args,release))
$(eval $(call build_deps_args,snapshot))

#----------------------------------------------------------------------------------------------

ifeq ($(OS),macos)
EMBEDDED_LIBS_FLAGS=$(foreach L,$(EMBEDDED_LIBS),-Wl,-force_load,$(L))
else
EMBEDDED_LIBS_FLAGS=\
	-Wl,-Bstatic $(HIREDIS) $(LIBEVENT) -Wl,-Bdynamic \
	-Wl,--whole-archive $(EMBEDDED_LIBS) -Wl,--no-whole-archive \
	-lssl -lcrypt
endif

define extract_symbols
$(SHOW)objcopy --only-keep-debug $1 $1.debug
$(SHOW)objcopy --strip-debug $1
$(SHOW)objcopy --add-gnu-debuglink $1.debug $1
endef

$(TARGET): $(MISSING_DEPS) $(OBJECTS) $(BINDIR)/release-deps.o $(BINDIR)/snapshot-deps.o
	@echo Linking $@...
	$(SHOW)$(CC) -shared -o $@ $(OBJECTS) $(BINDIR)/release-deps.o $(LD_FLAGS) $(EMBEDDED_LIBS_FLAGS)
	$(SHOW)mkdir -p $(dir $@)/snapshot
	$(SHOW)$(CC) -shared -o $(dir $@)/snapshot/$(notdir $@) $(OBJECTS) $(BINDIR)/snapshot-deps.o $(LD_FLAGS) $(EMBEDDED_LIBS_FLAGS)
#ifneq ($(DEBUG),1)
#	$(call extract_symbols,$@)
#	$(call extract_symbols,$(dir $@)/snapshot/$(notdir $@))
#endif
	$(SHOW)$(READIES)/bin/symlink -f -d $(ROOT) -t $(TARGET)

static: $(TARGET:.so=.a)

$(TARGET:.so=.a): $(OBJECTS) $(LIBEVENT) $(LIBPYTHON) $(HIREDIS)
	@echo Creating $@...
	$(SHOW)$(AR) rcs $@ $(filter-out module_init,$(OBJECTS)) $(LIBEVENT) $(HIREDIS)

#----------------------------------------------------------------------------------------------

# we avoid $(SUDO) here since we expect 'sudo make setup'

setup:
	@echo Setting up system...
	$(SHOW)./deps/readies/bin/getpy3
	$(SHOW)python3 ./system-setup.py

#----------------------------------------------------------------------------------------------

ifeq ($(DEPS),1)

deps: $(LIBPYTHON) $(LIBEVENT) $(HIREDIS) $(GEARS_PYTHON)

#----------------------------------------------------------------------------------------------

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

gears_python: $(GEARS_PYTHON)

$(GEARS_PYTHON):
	$(SHOW)make --no-print-directory -C plugins/python

#----------------------------------------------------------------------------------------------

else

deps: ;

endif # DEPS

#----------------------------------------------------------------------------------------------

ifeq ($(DIAG),1)
$(info *** MK_CLEAN_DIR=$(MK_CLEAN_DIR))
$(info *** GEARS_PYTHON=$(GEARS_PYTHON))
$(info *** LIBEVENT=$(LIBEVENT))
$(info *** HIREDIS=$(HIREDIS))
$(info *** MISSING_DEPS=$(MISSING_DEPS))
endif

clean: clean-gears-python
	$(SHOW)rm -rf $(BINDIR) $(TARGET) $(TARGET.snapshot) $(notdir $(TARGET)) $(BINROOT)/redislabs
	-$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	$(SHOW)rm -f $(TARGET) $(TARGET.snapshot) $(TARGET:.so=.a) $(notdir $(TARGET)) \
		artifacts/release/$(GEARS_PYTHON_TAR.release)* artifacts/snapshot/$(DGEARS_PYTHON_TAR.snapshot)*
ifeq ($(DEPS),1)
	${MAKE} -C build/hiredis clean
	${MAKE} -C build/libevent clean
	${MAKE} -C plugins/python clean
	${MAKE} -C plugins/jvmplugin/src clean
endif

jvmplugin:
	@echo Building jvmplugin...
	${MAKE} -C plugins/jvmplugin PYTHONDIR=$(BINROOT)/python3_$(GEARS_VERSION)

clean-gears-python:
	$(SHOW)make clean -C plugins/python

#----------------------------------------------------------------------------------------------

ifeq ($(WITH_PYTHON),1)
RAMP_OPT += GEARSPY_PATH=$(abspath $(GEARS_PYTHON))
endif

artifacts/release/$(RAMP.release) artifacts/snapshot/$(RAMP.snapshot) : $(TARGET) ramp.yml jvmplugin
	@echo Packing module...
	$(SHOW)RAMP=1 SYM=0 VARIANT=$(RAMP_VARIANT) $(RAMP_OPT) ./pack.sh $(TARGET)
	python3 append_deps.py

artifacts/release/$(GEARS_PYTHON_TAR.release) artifacts/snapshot/$(GEARS_PYTHON_TAR.snapshot): $(CPYTHON_PREFIX) $(GEARS_PYTHON)
	@echo Packing Python plugin...
	$(SHOW)RAMP=0 GEARSPY=1 CPYTHON_PREFIX=$(CPYTHON_PREFIX) GEARSPY_PATH=$(abspath $(GEARS_PYTHON)) ./pack.sh $(TARGET)

ramp ramp-pack: artifacts/release/$(RAMP.release) artifacts/snapshot/$(RAMP.snapshot)

# keeping jvmplugin in case it's called explicitly. worst case it regenerates
pack: artifacts/release/$(RAMP.release) artifacts/snapshot/$(RAMP.snapshot) \
		artifacts/release/$(GEARS_PYTHON_TAR.release) artifacts/snapshot/$(GEARS_PYTHON_TAR.snapshot) \
		jvmplugin

verify-packs:
	@set -e ;\
	MOD=`./deps/readies/bin/redis-cmd --loadmodule $(TARGET) Plugin $(GEARS_PYTHON) -- RG.CONFIGGET DependenciesSha256 2> /tmp/redisgears-sha.err` ;\
	if [[ -z $MOD ]]; then \
		>&2 cat /tmp/redisgears-sha.err ;\
		exit 1 ;\
	fi ;\
	REL=`cat artifacts/release/$(GEARS_PYTHON_TAR.release).sha256` ;\
	SNAP=`cat artifacts/snapshot/$(GEARS_PYTHON_TAR.snapshot).sha256` ;\
	if [[ $$MOD != $$REL || $$REL != $$SNAP ]]; then \
		>&2 echo "Module and package SHA256 don't match." ;\
		>&2 echo "\"$$MOD\"" ;\
		>&2 echo "\"$$REL\"" ;\
		>&2 echo "\"$$SNAP\"" ;\
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

ifeq ($(VALGRIND),1)
TEST_FLAGS += VALGRIND=1
endif

ifeq ($(OS),macos)
PARALLELISM?=1
else
PARALLELISM?=4
endif

test: __sep
ifneq ($(TEST),)
	@set -e; \
	cd pytest; \
	BB=1 $(TEST_FLAGS) python3 -m RLTest --test $(TEST) $(TEST_ARGS) \
		$(RLTEST_GDB) -s -v --module $(abspath $(TARGET)) \
		--module-args "Plugin $(abspath $(GEARS_PYTHON))"
else
	$(SHOW)set -e; \
	cd pytest; \
	$(TEST_FLAGS) MOD=$(abspath $(TARGET)) GEARSPY_PATH=$(abspath $(GEARS_PYTHON)) ./run_tests.sh --parallelism $(PARALLELISM)
	${MAKE} -C plugins/jvmplugin tests PYTHONDIR=$(PWD)/$(BINROOT)/python3_$(GEARS_VERSION)
endif

#----------------------------------------------------------------------------------------------

platform:
	$(SHOW)make -C build/docker build

coverage_report:
	gcovr -r . --html --html-details -o result.html -e "src/utils/*" -e "src/*.h" -e "deps/*" -e "plugins/python/redisai.h"
