
# built-in rules are needed here
# MAKEFLAGS syntax differs in gnu make v3 and v4
override MAKEFLAGS:=$(filter-out r -r no-builtin-rules --no-builtin-rules,$(MAKEFLAGS))

ROOT=../..
include $(ROOT)/deps/readies/mk/main

MK.configure=1
GEARS_VERSION:=$(shell $(ROOT)/getver)

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/cpython

CONFIGURE_BUILD_DIR=$(BINDIR)

SRCDIR=$(ROOT)/deps/cpython

CONFIGURE_TARGET=$(BINDIR)/libpython3.7m.a
CONFIGURE_PYTHON=$(BINDIR)/python
TARGET=$(BINDIR)/libpython3.7m-fixed.a

MK_ALL_TARGETS=bindirs build

export CPYTHON_PREFIX=$(abspath $(BINROOT)/python3_$(GEARS_VERSION))
MK_CLEAN_ALL_DIRS += $(CPYTHON_PREFIX)
LIBPYTHON=$(CPYTHON_PREFIX)/lib/libpython3.7m.a
CPYTHON=$(CPYTHON_PREFIX)/bin/python3

include $(MK)/defs

#----------------------------------------------------------------------------------------------

export CFLAGS=\
	-fPIC \
	-Wno-nullability-completeness \
	-Wno-expansion-to-defined

ifeq ($(OS),macosx)
BINUTILS_PREFIX:=$(shell brew --prefix binutils)
OBJCOPY:=$(BINUTILS_PREFIX)/bin/objcopy
else
OBJCOPY:=objcopy
endif

#----------------------------------------------------------------------------------------------

CONFIGURE_FLAGS += \
	--prefix=$(CPYTHON_PREFIX)

#----------------------------------------------------------------------------------------------

include $(MK)/rules

$(TARGET): $(CONFIGURE_TARGET)
	@echo Fixing $@ ...
	$(SHOW)cp $< $@
# ifeq ($(OS),macosx)
# 	$(SHOW)cp $(BINDIR)/Python/pystate.o $(BINDIR)/
# 	$(SHOW)$(OBJCOPY) --localize-symbol _PyGILState_Ensure --localize-symbol _PyGILState_Release $(BINDIR)/pystate.o
# 	$(SHOW)ar rs $@ $(BINDIR)/pystate.o
# 	$(SHOW)rm $(BINDIR)/pystate.o
# else
# 	$(SHOW)$(OBJCOPY) --localize-symbol PyGILState_Ensure --localize-symbol PyGILState_Release $@
# endif
	@echo Installing Python...
ifneq ($(VERBOSE),1)
	$(SHOW)make -C $(CONFIGURE_BUILD_DIR) install 2>&1 > $(abspath $(CONFIGURE_BUILD_DIR))/python-install.log
else
	$(SHOW)make -C $(CONFIGURE_BUILD_DIR) install
endif
	$(SHOW)PYTHON=$(CPYTHON) ./install-pyextras
