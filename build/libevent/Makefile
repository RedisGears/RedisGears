
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
BINROOT=$(ROOT)/bin/$(FULL_VARIANT)

define __SEP
import os; rows, cols = os.popen('stty size', 'r').read().split(); print(\"\n\" + '-' * (int(cols) - 1) + \"\n\")
endef

#----------------------------------------------------------------------------------------------

LIBEVENT_BRANCH=release-2.1.8-stable

BINDIR=$(BINROOT)/libevent

SRCDIR=$(ROOT)/deps/libevent

CONFIGURE_FLAGS=

#----------------------------------------------------------------------------------------------

BIN_DIRS=$(BINDIR)

define mkdir_rule
$(1):
	$$(SHOW)mkdir -p $(1)
endef

#----------------------------------------------------------------------------------------------

export CFLAGS=-fPIC

.PHONY: all bindirs build source

all: bindirs build

__sep: ;
#	@python -c "$(__SEP)"

bindirs: $(BIN_DIRS)

$(foreach DIR,$(BIN_DIRS),$(eval $(call mkdir_rule,$(DIR))))

build: __sep bindirs
ifeq (,$(wildcard $(BINDIR)/Makefile))
	$(SHOW)cd $(BINDIR); $(realpath $(SRCDIR))/configure $(CONFIGURE_FLAGS)
endif
	@make -C $(BINDIR) -j

clean: __sep
ifeq ($(ALL),1) 
	$(SHOW)rm -rf $(BINDIR)
else
	$(SHOW)$(MAKE) clean -C $(BINDIR)
endif

#----------------------------------------------------------------------------------------------

source: $(SRCDIR) $(SRCDIR)/configure

$(SRCDIR):
	$(SHOW)cd $(dir $(SRCDIR)); \
		git clone --single-branch --branch $(LIBEVENT_BRANCH) https://github.com/libevent/libevent.git

$(SRCDIR)/configure: $(SRCDIR)
	$(SHOW)cd $(SRCDIR); ./autogen.sh
