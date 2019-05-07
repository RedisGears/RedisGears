
# built-in rules are needed here
# MAKEFLAGS syntax differs in gnu make v3 and v4
override MAKEFLAGS:=$(filter-out r -r no-builtin-rules --no-builtin-rules,$(MAKEFLAGS))

ROOT=../..
include $(ROOT)/build/mk/defs

# include Makefile.defs

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/cpython
BUILD_DIR=$(BINDIR)

SRCDIR=$(ROOT)/deps/cpython

export CPYTHON_PREFIX=/opt/redislabs/lib/modules/python27
PYENV_DIR=$(CPYTHON_PREFIX)/.venv

PYTHON_ENCODING ?= ucs4

#----------------------------------------------------------------------------------------------

ifeq ($(OS),macosx)

ZLIB_PREFIX:=$(shell brew --prefix zlib)
LIBSSL_PREFIX:=$(shell brew --prefix openssl)
READLINE_PREFIX:=$(shell brew --prefix readline)
SDK_PREFIX:=$(shell xcrun --show-sdk-path)

export CPPFLAGS:=\
	-I$(ZLIB_PREFIX)/include \
	-I$(READLINE_PREFIX)/include \
	-I$(LIBSSL_PREFIX)/include \
	-I$(SDK_PREFIX)/usr/include

export LDFLAGS:=\
	-L$(ZLIB_PREFIX)/lib \
	-L$(LIBSSL_PREFIX)/lib \
	-L$(READLINE_PREFIX)/lib \
	-L$(SDK_PREFIX)/usr/lib

export PKG_CONFIG_PATH:=$(ZLIB_PREFIX)/lib/pkgconfig:$(LIBSSL_PREFIX)/lib/pkgconfig:$(READLINE_PREFIX)/lib/pkgconfig

CONFIGURE_FLAGS.macosx += CPPFLAGS='$(CPPFLAGS)' LDFLAGS='$(LDFLAGS)'

endif # macosx

#----------------------------------------------------------------------------------------------

CONFIGURE_FLAGS += \
	$(if $(eq $(PYTHON_ENCODING),),,--enable-unicode=$(PYTHON_ENCODING)) \
	--prefix=$(CPYTHON_PREFIX) \
	$(CONFIGURE_FLAGS.$(OS))

#----------------------------------------------------------------------------------------------

export CFLAGS=-fPIC
# -DPy_UNICODE_WIDE

#----------------------------------------------------------------------------------------------

.PHONY: all build pyenv

all: build pyenv

build: __sep bindirs
ifeq (,$(wildcard $(BUILD_DIR)/Makefile))
	$(SHOW)cd $(BUILD_DIR); $(realpath $(SRCDIR))/configure $(CONFIGURE_FLAGS)
endif
	@make -C $(BUILD_DIR)

clean:
ifeq ($(ALL),1) 
	$(SHOW)rm -rf $(BUILD_DIR) $(CPYTHON_PREFIX)
else
	$(SHOW)$(MAKE) clean -C $(BUILD_DIR)
endif

include $(MK)/rules

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
	pipenv install --python $(CPYTHON_PREFIX)/bin/python3.7
	$(SHOW)cp $(CPYTHON_PREFIX)/Pipfile.lock .
