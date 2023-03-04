
ROOT=../..
include $(ROOT)/deps/readies/mk/main

# OSNICK=bullseye|centos7|centos8|xenial|bionic
OSNICK ?= bullseye

REPO=redisgears

REDIS_VER=6.2.10

INT_BRANCHES=1.2 1.0

ART_DIR=$(ROOT)/bin/artifacts
ART_INT_DIR=/var/opt/redislabs/artifacts

include $(MK)/docker.native
