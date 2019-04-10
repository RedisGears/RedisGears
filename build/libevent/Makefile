
ROOT=../..
include $(ROOT)/build/Makefile.defs

#----------------------------------------------------------------------------------------------

LIBEVENT_BRANCH=release-2.1.8-stable

BINDIR=$(BINROOT)/libevent

SRCDIR=$(ROOT)/deps/libevent

CONFIGURE_FLAGS=

#----------------------------------------------------------------------------------------------

export CFLAGS=-fPIC

.PHONY: all build source

all: build

build: __sep bindirs
ifeq (,$(wildcard $(BINDIR)/Makefile))
	$(SHOW)cd $(BINDIR); $(realpath $(SRCDIR))/configure $(CONFIGURE_FLAGS)
endif
	@make -C $(BINDIR) -j

clean:
ifeq ($(ALL),1) 
	$(SHOW)rm -rf $(BINDIR)
else
	$(SHOW)$(MAKE) clean -C $(BINDIR)
endif

include $(ROOT)/build/Makefile.rules

#----------------------------------------------------------------------------------------------

source: $(SRCDIR) $(SRCDIR)/configure

$(SRCDIR):
	$(SHOW)cd $(dir $(SRCDIR)); \
		git clone --single-branch --branch $(LIBEVENT_BRANCH) https://github.com/libevent/libevent.git

$(SRCDIR)/configure: $(SRCDIR)
	$(SHOW)cd $(SRCDIR); ./autogen.sh
