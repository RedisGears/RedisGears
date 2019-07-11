
ROOT=../..
include $(ROOT)/build/mk/defs

#----------------------------------------------------------------------------------------------

LIBEVENT_BRANCH=release-2.1.8-stable

BINDIR=$(BINROOT)/libevent
BUILD_DIR=$(BINDIR)

SRCDIR=$(ROOT)/deps/libevent

#----------------------------------------------------------------------------------------------

export CFLAGS=-fPIC

.PHONY: all build source

all: build

build: __sep bindirs $(SRCDIR)/configure
ifeq (,$(wildcard $(BUILD_DIR)/Makefile))
	$(SHOW)cd $(BUILD_DIR); $(realpath $(SRCDIR))/configure $(CONFIGURE_FLAGS)
endif
	@make -C $(BUILD_DIR) -j $$(nproc)

clean:
ifeq ($(ALL),1) 
	$(SHOW)rm -rf $(BINDIR)
else
	$(SHOW)$(MAKE) clean -C $(BUILD_DIR)
endif

include $(MK)/rules

#----------------------------------------------------------------------------------------------

source: $(SRCDIR) $(SRCDIR)/configure

$(SRCDIR):
	$(SHOW)cd $(dir $(SRCDIR)); \
		git clone --single-branch --branch $(LIBEVENT_BRANCH) https://github.com/libevent/libevent.git

$(SRCDIR)/configure: $(SRCDIR)
	$(SHOW)cd $(SRCDIR); ./autogen.sh
