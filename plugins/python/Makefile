
ifeq ($(VG),1)
override VALGRIND:=1
endif

ifeq ($(VALGRIND),1)
override DEBUG:=1
endif

GCC=1

ROOT=../..
include $(ROOT)/deps/readies/mk/main

define HELPTEXT
make build
  DEBUG=1         # build debug variant
  VG|VALGRIND=1   # build with VALGRIND (implies DEBUG=1)
  VARIANT=name    # build as variant `name`
  DEPS=1          # also build dependant modules
make clean        # remove binary files
  ALL=1           # remove binary directories
  DEPS=1          # also clean dependant modules

make deps          # build dependant modules
make cpython       # build cpython
make all           # build all libraries and packages

endef

MK_ALL_TARGETS=build

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/gears_python
SRCDIR=$(abspath $(ROOT))

TARGET=$(BINDIR)/gears_python.so

#----------------------------------------------------------------------------------------------

MK_CUSTOM_CLEAN=1

include $(MK)/defs

DEPENDENCIES=cpython

ifneq ($(filter all deps $(DEPENDENCIES),$(MAKECMDGOALS)),)
DEPS=1
endif

#----------------------------------------------------------------------------------------------

CPYTHON_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/cpython
CPYTHON_BINROOT=$(ROOT)/bin/$(FULL_VARIANT.release)

include $(ROOT)/build/cpython/Makefile.defs

#----------------------------------------------------------------------------------------------

define _SOURCES
	plugins/python/redisgears_python.c
	plugins/python/globals.c
	src/utils/dict.c
	src/utils/thpool.c
	src/utils/buffer.c
endef
ifeq ($(DEBUG),1)
_SOURCES += src/debug.c
endif

SOURCES=$(addprefix $(SRCDIR)/,$(filter %,$(subst $(__NL), ,$(_SOURCES))))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES))

define _CC_FLAGS
	-fPIC
	-std=gnu99
	-MMD
	-MF $(@:.o=.d)
	$(CC_FLAGS.coverage)

	-include common.h
	-I$(CPYTHON_DIR)/Include
	-I$(CPYTHON_DIR)
	-I$(ROOT)
	-I$(ROOT)/deps
	-I$(SRCDIR)/src
	-I$(BINDIR)
	-I$(ROOT)/bin/$(FULL_VARIANT.release)/cpython

	-DWITHPYTHON
	-DREDISMODULE_EXPERIMENTAL_API
	-DCPYTHON_PATH=\"$(abspath $(CPYTHON_BINROOT))/\"
endef
CC_FLAGS += $(filter %,$(subst $(__NL), ,$(_CC_FLAGS)))

ifeq ($(VALGRIND),1)
CC_FLAGS += -DVALGRIND
endif

ifeq ($(DEBUG),1)
CC_FLAGS += -g -O0 -DDEBUG
LD_FLAGS += -g
else
CC_FLAGS += -O2 -Wno-unused-result
endif

ifeq ($(GCOV),1)
CC_FLAGS += -fprofile-arcs -ftest-coverage
LD_FLAGS += -fprofile-arcs -ftest-coverage
endif

ifeq ($(OS),macos)
LD_FLAGS += -undefined dynamic_lookup
endif

LD_FLAGS += $(LD_FLAGS.coverage)

#----------------------------------------------------------------------------------------------
# cpython-related definitions
#----------------------------------------------------------------------------------------------

CPYTHON_DIR=$(ROOT)/deps/cpython

LD_FLAGS +=

EMBEDDED_LIBS += $(LIBPYTHON)

ifeq ($(wildcard $(LIBPYTHON)),)
MISSING_DEPS += $(LIBPYTHON)
endif

ifeq ($(OS),macos)
LD_FLAGS += \
	$(gettext_prefix)/lib/libintl.a \
	-liconv
else
EMBEDDED_LIBS += -lutil -luuid
endif

ifeq ($(OS),macos)
EMBEDDED_LIBS_FLAGS=$(foreach L,$(EMBEDDED_LIBS),-Wl,-force_load,$(L))
else
EMBEDDED_LIBS_FLAGS=\
	-Wl,-Bstatic  -Wl,-Bdynamic \
	-Wl,--whole-archive $(EMBEDDED_LIBS) -Wl,--no-whole-archive
endif

#----------------------------------------------------------------------------------------------

include $(MK)/rules

-include $(CC_DEPS)

#----------------------------------------------------------------------------------------------

$(SRCDIR)/plugins/python/redisgears_python.c : $(BINDIR)/GearsBuilder.auto.h $(BINDIR)/cloudpickle.auto.h

$(BINDIR)/GearsBuilder.auto.h: GearsBuilder.py
	$(SHOW)xxd -i $< > $@

$(BINDIR)/cloudpickle.auto.h: cloudpickle.py
	$(SHOW)xxd -i $< > $@

#----------------------------------------------------------------------------------------------

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@

$(TARGET): $(MISSING_DEPS) $(OBJECTS)
	@echo Linking $@...
	$(SHOW)$(CC) -shared -o $@ $(OBJECTS) $(LD_FLAGS) $(EMBEDDED_LIBS_FLAGS)
	$(SHOW)$(READIES)/bin/symlink -f -d $(ROOT) -t $(TARGET)

#----------------------------------------------------------------------------------------------

ifeq ($(DEPS),1)

deps: $(LIBPYTHON)

cpython: $(LIBPYTHON)

$(LIBPYTHON):
	@echo Building cpython...
	$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/cpython DEBUG=

else

deps: ;

endif # DEPS

#----------------------------------------------------------------------------------------------

clean:
ifeq ($(ALL),1)
	$(SHOW)rm -rf $(BINDIR) $(TARGET)
else
	-$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	$(SHOW)rm -f $(TARGET)
endif
