
MAKEFLAGS += --no-builtin-rules --no-builtin-variables  --no-print-directory

ROOT=../..

ifeq ($(SHOW),1)
override SHOW:=
else
override SHOW:=@
endif

OS=linux
# OS := $(shell sh -c 'uname -s 2>/dev/null || echo not')
ARCH=x64

ifeq ($(DEBUG),1)
FLAVOR=debug
else
FLAVOR=release
endif

ifneq ($(VARIANT),)
override VARIANT:=-$(VARIANT)
endif
FULL_VARIANT:=$(OS)-$(ARCH)-$(FLAVOR)$(VARIANT)
BINDIR=$(ROOT)/bin/$(FULL_VARIANT)

define __SEP
import os; rows, cols = os.popen('stty size', 'r').read().split(); print(\"\n\" + '-' * (int(cols) - 1) + \"\n\")
endef

#----------------------------------------------------------------------------------------------

BUILD_DIR=$(BINDIR)/cpython

SRCDIR=$(ROOT)/deps/cpython

export CPYTHON_PREFIX=/opt/redislabs/lib/modules/python27
PYENV_DIR=$(CPYTHON_PREFIX)/.venv

PYTHON_ENCODING ?= ucs2

CONFIGURE_FLAGS= \
	--without-pymalloc \
	$(if $(eq $(PYTHON_ENCODING),),,--enable-unicode=$(PYTHON_ENCODING)) \
	--prefix=$(CPYTHON_PREFIX)

#----------------------------------------------------------------------------------------------

BIN_DIRS=$(BUILD_DIR)

define mkdir_rule
$(1):
	$$(SHOW)mkdir -p $(1)
endef

#----------------------------------------------------------------------------------------------

export CFLAGS=-fPIC -DREDIS_ALLOC
# -DPy_UNICODE_WIDE

.PHONY: all bindirs build pyenv

all: bindirs build pyenv

__sep: ;
#	@python -c "$(__SEP)"

bindirs: $(BIN_DIRS)

$(foreach DIR,$(BIN_DIRS),$(eval $(call mkdir_rule,$(DIR))))

build: __sep bindirs
ifeq (,$(wildcard $(BUILD_DIR)/Makefile))
	$(SHOW)cd $(BUILD_DIR); $(realpath $(SRCDIR))/configure $(CONFIGURE_FLAGS)
endif
	@make -C $(BUILD_DIR) -j

clean: __sep
ifeq ($(ALL),1) 
	$(SHOW)rm -rf $(BUILD_DIR) $(CPYTHON_PREFIX)
else
	$(SHOW)$(MAKE) clean -C $(BUILD_DIR)
endif

#----------------------------------------------------------------------------------------------

pyenv: $(CPYTHON_PREFIX) $(PYENV_DIR)

$(CPYTHON_PREFIX):
	$(SHOW)make -C $(BUILD_DIR) install 2>&1 >$(BUILD_DIR)/python-install.log

$(PYENV_DIR): $(CPYTHON_PREFIX)
	$(SHOW)cp Pipfile* $(CPYTHON_PREFIX)
	$(SHOW)cd $(CPYTHON_PREFIX); \
	export PIPENV_VENV_IN_PROJECT=1; \
	export LC_ALL=C.UTF-8; \
	export LANG=C.UTF-8; \
	pipenv install --python $(CPYTHON_PREFIX)/bin/python
	$(SHOW)cp $(CPYTHON_PREFIX)/Pipfile.lock .
