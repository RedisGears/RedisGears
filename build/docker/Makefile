MK.pyver:=3
ROOT=../..
include $(ROOT)/deps/readies/mk/main

# OSNICK=bullseye|centos7|centos8|xenial|bionic
OSNICK ?= bullseye
ifeq ($(OSNICK),centos8)  # temporary, until one can make OSNICK=ol8.5 with all dockers from redisfab
OS.custom=1
OS.centos8=oraclelinux:8
endif

REPO=redisgears

REDIS_VER=6.2.5

INT_BRANCHES=1.2 1.0

ART_DIR=$(ROOT)/artifacts
ART_INT_DIR=/var/opt/redislabs/artifacts

include $(MK)/docker.native
