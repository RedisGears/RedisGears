
ROOT=../..

PLATFORMS=centos7 fedora xenial bionic stretch buster

REPO=localhost/redisgears

OS.centos7=centos:7.6.1810
OS.fedora=fedora:30
OS.xenial=ubuntu:xenial
OS.bionic=ubuntu:bionic
OS.stretch=debian:stretch
OS.buster=debian:buster

ARGS=\
	-f $(ROOT)/Dockerfile \
	--build-arg TEST=1 \
	--build-arg PACK=1 \
	$(ROOT)

define build_platform
$(1):
	@docker build $(BUILD_OPT) -t $(REPO):$(1) --build-arg OSNICK=$(1) --build-arg OS=$(OS.$(1)) $(ARGS)

.PHONY: $(1)
endef

platforms:
	@echo "Platforms: $(PLATFORMS)"

all: $(PLATFORMS)

$(foreach P,$(PLATFORMS),$(eval $(call build_platform,$(P))))
