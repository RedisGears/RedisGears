
ROOT=../..
include $(ROOT)/build/mk/defs

#----------------------------------------------------------------------------------------------

LIBEVENT_BRANCH=release-2.1.8-stable

BINDIR=$(BINROOT)/libevent
BUILD_DIR=$(BINDIR)

SRCDIR=$(ROOT)/deps/libevent

#----------------------------------------------------------------------------------------------

export CFLAGS += -fPIC

ifeq ($(OS),macosx)

ZLIB_PREFIX:=$(shell brew --prefix zlib)
LIBSSL_PREFIX:=$(shell brew --prefix openssl)
READLINE_PREFIX:=$(shell brew --prefix readline)
SDK_PREFIX:=$(shell xcrun --show-sdk-path)

export CFLAGS += \
	-I$(ZLIB_PREFIX)/include \
	-I$(READLINE_PREFIX)/include \
	-I$(LIBSSL_PREFIX)/include \
	-I$(SDK_PREFIX)/usr/include

export LDFLAGS += \
	-L$(ZLIB_PREFIX)/lib \
	-L$(LIBSSL_PREFIX)/lib \
	-L$(READLINE_PREFIX)/lib \
	-L$(SDK_PREFIX)/usr/lib

export PKG_CONFIG_PATH:=$(ZLIB_PREFIX)/lib/pkgconfig:$(LIBSSL_PREFIX)/lib/pkgconfig:$(READLINE_PREFIX)/lib/pkgconfig

CONFIGURE_FLAGS.macosx += CFLAGS='$(CFLAGS)' CPPFLAGS='$(CFLAGS)' LDFLAGS='$(LDFLAGS)'

endif # macosx

CONFIGURE_FLAGS += \
	$(CONFIGURE_FLAGS.$(OS))

#----------------------------------------------------------------------------------------------

ifeq ($(OS),macosx)
NPROC:=$(shell sysctl -n hw.physicalcpu)
else
NPROC:=$(shell nproc)
endif

.PHONY: all build source

all: build

build: __sep bindirs $(SRCDIR)/configure
ifeq (,$(wildcard $(BUILD_DIR)/Makefile))
	$(SHOW)cd $(BUILD_DIR); $(realpath $(SRCDIR))/configure $(CONFIGURE_FLAGS)
endif
	@make -C $(BUILD_DIR) -j $(NPROC)

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
