
ROOT=../..
include $(ROOT)/deps/readies/mk/main

MK.configure=1

define HELP
make build    # configure and compile
make clean    # clean generated sbinaries
  ALL=1       # remote entire binary directory
make source   # fetch sources and generate configure script
endef

#----------------------------------------------------------------------------------------------

LIBEVENT_REPO=https://github.com/libevent/libevent.git
LIBEVENT_BRANCH=release-2.1.8-stable

BINDIR=$(BINROOT)/libevent

SRCDIR=$(ROOT)/deps/libevent

TARGET=$(BINDIR)/.libs/libevent.a

export CFLAGS=-fPIC

include $(MK)/defs

#----------------------------------------------------------------------------------------------

include $(MK)/rules

source: $(SRCDIR) $(SRCDIR)/configure

.PHONY: source

$(SRCDIR):
	$(SHOW)cd $(dir $(SRCDIR)); \
		git clone --single-branch --branch $(LIBEVENT_BRANCH) $(LIBEVENT_REPO)

$(SRCDIR)/configure:
	$(SHOW)cd $(SRCDIR); ./autogen.sh
