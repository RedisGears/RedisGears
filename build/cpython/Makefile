
# built-in rules are needed here
# MAKEFLAGS syntax differs in gnu make v3 and v4
override MAKEFLAGS:=$(filter-out r -r no-builtin-rules --no-builtin-rules,$(MAKEFLAGS))

ROOT=../..
include $(ROOT)/deps/readies/mk/main

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/cpython
CONFIGURE_BUILD_DIR=$(BINDIR)

SRCDIR=$(ROOT)/deps/cpython

# REDISLABS_DIR=$(BINROOT)/opt/redislabs

export CPYTHON_PREFIX=/opt/redislabs/lib/modules/python3
PYENV_DIR=$(CPYTHON_PREFIX)/.venv

PYTHON_ENCODING ?= ucs4

include $(MK)/defs

ifeq ($(OS),macosx)
BINUTILS_PREFIX:=$(shell brew --prefix binutils)
OBJCOPY:=$(BINUTILS_PREFIX)/bin/objcopy
else
OBJCOPY:=objcopy
endif

#----------------------------------------------------------------------------------------------

CONFIGURE_FLAGS += \
	$(if $(eq $(PYTHON_ENCODING),),,--enable-unicode=$(PYTHON_ENCODING)) \
	--prefix=$(CPYTHON_PREFIX) \
	$(CONFIGURE_FLAGS.$(OS))

#----------------------------------------------------------------------------------------------

TARGET=$(BINDIR)/libpython3.7m-fixed.a

export CFLAGS=-fPIC \
	-Wno-nullability-completeness -Wno-expansion-to-defined

#----------------------------------------------------------------------------------------------

.PHONY: all pyenv

all: build pyenv


$(BINDIR)/libpython3.7m.a: __sep bindirs
ifeq (,$(wildcard $(CONFIGURE_BUILD_DIR)/Makefile))
	$(SHOW)cd $(CONFIGURE_BUILD_DIR); $(realpath $(SRCDIR))/configure $(CONFIGURE_FLAGS)
endif
	$(SHOW)make -C $(CONFIGURE_BUILD_DIR) -j $(NPROC)

$(TARGET): $(BINDIR)/libpython3.7m.a
	@echo Fixing libpython3.7m.a ...
	$(SHOW)cp $(BINDIR)/libpython3.7m.a $(TARGET)
ifeq ($(OS),macosx)
	cp $(BINDIR)/Python/pystate.o $(BINDIR)/
	$(SHOW)$(OBJCOPY) --localize-symbol _PyGILState_Ensure --localize-symbol _PyGILState_Release $(BINDIR)/pystate.o
	$(SHOW)ar rs $(TARGET) $(BINDIR)/pystate.o
	$(SHOW)rm $(BINDIR)/pystate.o
else
	$(SHOW)$(OBJCOPY) --localize-symbol PyGILState_Ensure --localize-symbol PyGILState_Release $(TARGET)
endif

clean:
ifeq ($(ALL),1) 
	$(SHOW)rm -rf $(CONFIGURE_BUILD_DIR) $(CPYTHON_PREFIX)
else
	$(SHOW)$(MAKE) clean -C $(CONFIGURE_BUILD_DIR)
endif

include $(MK)/rules

#----------------------------------------------------------------------------------------------

ifeq ($(OS),macosx)
/opt/redislabs:
	$(SHOW)if [ ! -d /opt/redislabs ]; then \
		sudo mkdir -p /opt/redislabs; \
		sudo chown $$USER /opt/redislabs; \
	fi

#/opt/redislabs:
# 	$(SHOW)mkdir -p $(REDISLABS_DIR)
# 	$(SHOW)sudo ln -sfn $(abspath $(REDISLABS_DIR)) /opt/redislabs
else
/opt/redislabs: ;
endif

pyenv: /opt/redislabs $(CPYTHON_PREFIX) $(PYENV_DIR)

$(CPYTHON_PREFIX): /opt/redislabs
	@echo Installing Python...
	$(SHOW)make -C $(CONFIGURE_BUILD_DIR) install 2>&1 > $(abspath $(CONFIGURE_BUILD_DIR))/python-install.log

$(PYENV_DIR): /opt/redislabs $(CPYTHON_PREFIX)
	$(SHOW)cp Pipfile* $(CPYTHON_PREFIX)
	$(SHOW)cd $(CPYTHON_PREFIX); \
	export PIPENV_VENV_IN_PROJECT=1; \
	export LC_ALL=C.UTF-8; \
	export LANG=C.UTF-8; \
	pipenv --site-packages install --python $(CPYTHON_PREFIX)/bin/python3
	$(SHOW)cp $(CPYTHON_PREFIX)/Pipfile.lock .
	$(SHOW)echo /usr/local/lib/python3.7/site-packages > $(PYENV_DIR)/lib/python3.7/site-packages/local.pth
