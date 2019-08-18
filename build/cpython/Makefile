
# built-in rules are needed here
# MAKEFLAGS syntax differs in gnu make v3 and v4
override MAKEFLAGS:=$(filter-out r -r no-builtin-rules --no-builtin-rules,$(MAKEFLAGS))

ROOT=../..
include $(ROOT)/build/mk/defs

# include Makefile.defs

ifeq ($(OS),macosx)
export PATH:=$(PATH):$(HOME)/Library/Python/2.7/bin
endif

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/cpython
BUILD_DIR=$(BINDIR)

SRCDIR=$(ROOT)/deps/cpython

# REDISLABS_DIR=$(BINROOT)/opt/redislabs

export CPYTHON_PREFIX=/opt/redislabs/lib/modules/python3
PYENV_DIR=$(CPYTHON_PREFIX)/.venv

PYTHON_ENCODING ?= ucs4

#----------------------------------------------------------------------------------------------

ifeq ($(OS),macosx)

ZLIB_PREFIX:=$(shell brew --prefix zlib)
LIBSSL_PREFIX:=$(shell brew --prefix openssl)
READLINE_PREFIX:=$(shell brew --prefix readline)
SDK_PREFIX:=$(shell xcrun --show-sdk-path)

export CFLAGS:=\
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

CONFIGURE_FLAGS.macosx += CFLAGS='$(CFLAGS)' CPPFLAGS='$(CFLAGS)' LDFLAGS='$(LDFLAGS)'

endif # macosx

ifeq ($(OS),macosx)
OBJCOPY=/usr/local/opt/binutils/bin/objcopy
else
OBJCOPY=objcopy
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

.PHONY: all build pyenv

all: build pyenv

build: $(TARGET)

$(BINDIR)/libpython3.7m.a: __sep bindirs
ifeq (,$(wildcard $(BUILD_DIR)/Makefile))
	$(SHOW)cd $(BUILD_DIR); $(realpath $(SRCDIR))/configure $(CONFIGURE_FLAGS)
endif
	$(SHOW)make -C $(BUILD_DIR) -j $$(nproc)

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
	$(SHOW)rm -rf $(BUILD_DIR) $(CPYTHON_PREFIX)
else
	$(SHOW)$(MAKE) clean -C $(BUILD_DIR)
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
# 	$(SHOW)sudo ln -sfn $(realpath $(REDISLABS_DIR)) /opt/redislabs
else
/opt/redislabs: ;
endif

pyenv: /opt/redislabs $(CPYTHON_PREFIX) $(PYENV_DIR)

$(CPYTHON_PREFIX): /opt/redislabs
	@echo Installing Python...
	$(SHOW)make -C $(BUILD_DIR) install 2>&1 > $(abspath $(BUILD_DIR))/python-install.log

$(PYENV_DIR): /opt/redislabs $(CPYTHON_PREFIX)
	$(SHOW)cp Pipfile* $(CPYTHON_PREFIX)
	$(SHOW)cd $(CPYTHON_PREFIX); \
	export PIPENV_VENV_IN_PROJECT=1; \
	export LC_ALL=C.UTF-8; \
	export LANG=C.UTF-8; \
	pipenv --site-packages install --python $(CPYTHON_PREFIX)/bin/python3
	$(SHOW)cp $(CPYTHON_PREFIX)/Pipfile.lock .
	$(SHOW)echo /usr/local/lib/python3.7/site-packages > $(PYENV_DIR)/lib/python3.7/site-packages/local.pth
