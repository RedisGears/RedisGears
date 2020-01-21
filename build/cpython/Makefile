
# built-in rules are needed here
# MAKEFLAGS syntax differs in gnu make v3 and v4
override MAKEFLAGS:=$(filter-out r -r no-builtin-rules --no-builtin-rules,$(MAKEFLAGS))

ROOT=../..
include $(ROOT)/deps/readies/mk/main

MK.configure=1

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/cpython

CONFIGURE_BUILD_DIR=$(BINDIR)

SRCDIR=$(ROOT)/deps/cpython

CONFIGURE_TARGET=$(BINDIR)/libpython3.7m.a
CONFIGURE_PYTHON=$(BINDIR)/python
TARGET=$(BINDIR)/libpython3.7m-fixed.a

MK_ALL_TARGETS=bindirs build pyenv
MK_CLEAN_ALL_DIRS += $(BINDIR_CPYTHON_PREFIX) $(REDISLABS_CPYTHON_PREFIX)

REDISLABS_DIR=/opt/redislabs
export REDISLABS_CPYTHON_PREFIX=$(REDISLABS_DIR)/lib/modules/python3
BINDIR_CPYTHON_PREFIX=$(BINROOT)/redislabs/lib/modules/python3
REDISLABS_LIBPYTHON=$(REDISLABS_CPYTHON_PREFIX)/lib/libpython3.7m.a
REDISLABS_PYTHON=$(REDISLABS_CPYTHON_PREFIX)/bin/python3
PYENV_DIR=$(REDISLABS_CPYTHON_PREFIX)/.venv
PYENV_PYTHON=$(PYENV_DIR)/bin/python3

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

PYTHON_ENCODING ?= ucs4

CONFIGURE_FLAGS += \
	$(if $(eq $(PYTHON_ENCODING),),,--enable-unicode=$(PYTHON_ENCODING)) \
	--prefix=$(REDISLABS_CPYTHON_PREFIX)

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

#----------------------------------------------------------------------------------------------

$(REDISLABS_PYTHON) : $(CONFIGURE_PYTHON)
	@echo Installing Python...
	$(SHOW)SUDO=$(SUDO) LOCAL=$(BINDIR_CPYTHON_PREFIX) GLOBAL=$(REDISLABS_CPYTHON_PREFIX) PYENV_IN_SITU=$(PYENV_IN_SITU) ./prepare-python
	$(SHOW)make -C $(CONFIGURE_BUILD_DIR) install 2>&1 > $(abspath $(CONFIGURE_BUILD_DIR))/python-install.log

$(PYENV_PYTHON) : $(REDISLABS_PYTHON)
	$(SHOW)PYTHON_PATH=$(REDISLABS_CPYTHON_PREFIX) ./install-pyenv

ifneq ($(FIX_LINK),1)
pyenv: $(DEFAULT_TARGETS) $(PYENV_PYTHON)
else
pyenv:
	$(SHOW)$(SUDO) LOCAL=$(BINDIR_CPYTHON_PREFIX) GLOBAL=$(REDISLABS_CPYTHON_PREFIX) ./relink-pyenv
endif

clean-pyenv:
	$(SHOW)$(SUDO) LOCAL=$(BINDIR_CPYTHON_PREFIX) GLOBAL=$(REDISLABS_CPYTHON_PREFIX) ./clean-pyenv
	
.PHONY: pyenv clean-pyenv

#----------------------------------------------------------------------------------------------

ifeq ($(DIAG),1)
$(info *** CONFIGURE_PYTHON=$(CONFIGURE_PYTHON))
$(info *** CONFIGURE_TARGET=$(CONFIGURE_TARGET))
$(info pyenv : $(PYENV_PYTHON) : $(REDISLABS_PYTHON) : $(CONFIGURE_TARGET) $(CONFIGURE_PYTHON))
endif
