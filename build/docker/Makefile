
ROOT=../..
include $(ROOT)/deps/readies/mk/main

# OSNICK=bullseye|centos7|centos8|xenial|bionic
OSNICK ?= bullseye

REPO=redisgears

REDIS_VER=6.2.5

INT_BRANCHES=1.0

ART_DIR=$(ROOT)/artifacts
ART_INT_DIR=/var/opt/redislabs/artifacts

include $(MK)/docker.native
