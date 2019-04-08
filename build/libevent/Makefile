
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
_VARIANT:=$(OS)-$(ARCH)-$(FLAVOR)$(VARIANT)
BINDIR=$(ROOT)/bin/$(_VARIANT)

define __SEP
import os; rows, cols = os.popen('stty size', 'r').read().split(); print(\"\n\" + '-' * (int(cols) - 1) + \"\n\")
endef

#----------------------------------------------------------------------------------------------

LIBEVENT_BRANCH=release-2.1.8-stable

BUILD_DIR=$(BINDIR)/libevent

SRCDIR=$(ROOT)/deps/libevent

CONFIGURE_FLAGS=

#----------------------------------------------------------------------------------------------

BIN_DIRS=$(BUILD_DIR)

define mkdir_rule
$(1):
	$$(SHOW)mkdir -p $(1)
endef

#----------------------------------------------------------------------------------------------

export CFLAGS=-fPIC

.PHONY: all bindirs build

all: bindirs build automake

__sep:
	@python -c "$(__SEP)"

bindirs: $(BIN_DIRS)

$(foreach DIR,$(BIN_DIRS),$(eval $(call mkdir_rule,$(DIR))))

build: __sep bindirs
ifeq (,$(wildcard $(BUILD_DIR)/Makefile))
	$(SHOW)cd $(BUILD_DIR); $(realpath $(SRCDIR))/configure $(CONFIGURE_FLAGS)
endif
	@make -C $(BUILD_DIR)

clean:
ifeq ($(ALL),1) 
	$(SHOW)rm -rf $(BUILD_DIR)
else
	$(SHOW)$(MAKE) clean -C $(BUILD_DIR)
endif

#----------------------------------------------------------------------------------------------

automake:
	$(SHOW)if [ ! -d $(SRCDIR) ]; then \
		cd $(dir $(SRCDIR)); \
		git clone --single-branch --branch $(LIBEVENT_BRANCH) https://github.com/libevent/libevent.git; \
	fi
	$(SHOW)if [ ! -f $(SRCDIR)/configure ]; then \
		cd $(SRCDIR); \
		./autogen.sh; \
	fi
