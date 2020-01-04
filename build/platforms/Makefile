
ROOT=../..

PLATFORMS=centos7 fedora xenial bionic stretch buster

REPO=localhost/redisgears

OS.centos7=centos:7.6.1810
OS.fedora=fedora:30
OS.xenial=ubuntu:xenial
OS.bionic=ubuntu:bionic
OS.stretch=debian:stretch
OS.buster=debian:buster

ARCH=x64

TEST ?= 1
PACK ?= 1

ARTIFACTS=$(abspath ../../artifacts/release)

ARGS=\
	-f $(ROOT)/Dockerfile \
	--build-arg TEST=$(TEST) \
	--build-arg PACK=$(PACK) \
	$(ROOT)

define build_platform
$(1):
	@echo "Building for $(1) ..."
ifneq ($(VERBOSE),1)
	@docker build $(BUILD_OPT) -t $(REPO):$(1) --build-arg OSNICK=$(1) --build-arg OS=$(OS.$(1)) $(ARGS) > /tmp/docker.log 2>&1 ;\
	if [ $$$$? != 0 ]; then cat /tmp/docker.log; exit 1; fi
else
	@docker build $(BUILD_OPT) -t $(REPO):$(1) --build-arg OSNICK=$(1) --build-arg OS=$(OS.$(1)) $(ARGS)
endif
	@echo "Copying artifacts ..."
	@CID=$$$$(docker create $(REPO):$(1)) ;\
	docker cp $$$${CID}:/opt/redislabs/artifacts/redisgears.linux-$(1)-$(ARCH).99.99.99.zip $(ARTIFACTS)/ ;\
	docker cp $$$${CID}:/opt/redislabs/artifacts/redisgears-dependencies.linux-$(1)-$(ARCH).99.99.99.tgz $(ARTIFACTS)/ ;\
	docker rm -v $$$${CID} > /dev/null
	@echo "Done."

.PHONY: $(1)
endef

platforms:
	@echo "Platforms: $(PLATFORMS)"

all: $(PLATFORMS)

$(foreach P,$(PLATFORMS),$(eval $(call build_platform,$(P))))
