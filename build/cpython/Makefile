
ROOT=../..
include $(ROOT)/build/Makefile.defs

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/cpython
BUILD_DIR=$(BINDIR)

SRCDIR=$(ROOT)/deps/cpython

export CPYTHON_PREFIX=/opt/redislabs/lib/modules/python27
PYENV_DIR=$(CPYTHON_PREFIX)/.venv

PYTHON_ENCODING ?= ucs2

CONFIGURE_FLAGS= \
	--without-pymalloc \
	$(if $(eq $(PYTHON_ENCODING),),,--enable-unicode=$(PYTHON_ENCODING)) \
	--prefix=$(CPYTHON_PREFIX)

#----------------------------------------------------------------------------------------------

export CFLAGS=-fPIC -DREDIS_ALLOC
# -DPy_UNICODE_WIDE

#----------------------------------------------------------------------------------------------

.PHONY: all build pyenv

all: build pyenv

build: __sep bindirs
ifeq (,$(wildcard $(BUILD_DIR)/Makefile))
	$(SHOW)cd $(BUILD_DIR); $(realpath $(SRCDIR))/configure $(CONFIGURE_FLAGS)
endif
	@make -C $(BUILD_DIR) -j

clean:
ifeq ($(ALL),1) 
	$(SHOW)rm -rf $(BUILD_DIR) $(CPYTHON_PREFIX)
else
	$(SHOW)$(MAKE) clean -C $(BUILD_DIR)
endif

include $(ROOT)/build/Makefile.rules

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
