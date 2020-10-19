# BUILD redisfab/redisgears:${VERSION}-${ARCH}-${OSNICK}

ARG REDIS_VER=6.0.1

# OSNICK=bionic|stretch|buster
ARG OSNICK=buster

# OS=debian:buster-slim|debian:stretch-slim|ubuntu:bionic
ARG OS=debian:buster-slim

# ARCH=x64|arm64v8|arm32v7
ARG ARCH=x64

ARG PACK=0
ARG TEST=0

#----------------------------------------------------------------------------------------------
FROM redisfab/redis:${REDIS_VER}-${ARCH}-${OSNICK} AS redis
FROM ${OS} AS builder

ARG OSNICK
ARG OS
ARG ARCH
ARG REDIS_VER

RUN echo "Building for ${OSNICK} (${OS}) for ${ARCH}"

WORKDIR /build
COPY --from=redis /usr/local/ /usr/local/

ADD . /build

RUN ./deps/readies/bin/getpy2
RUN ./system-setup.py
RUN make fetch SHOW=1
RUN make all SHOW=1
RUN ./getver > artifacts/VERSION

ARG PACK
ARG TEST

RUN if [ "$PACK" = "1" ]; then make pack; fi
RUN if [ "$TEST" = "1" ]; then \
		TEST= make test TEST_ARGS="--verbose" ;\
		tar -C  /build/pytest/logs/ -czf /build/artifacts/pytest-logs-${ARCH}-${OSNICK}.tgz . ;\
	fi

#----------------------------------------------------------------------------------------------
FROM redisfab/redis:${REDIS_VER}-${ARCH}-${OSNICK}

ARG OSNICK
ARG OS
ARG ARCH
ARG REDIS_VER
ARG PACK

ENV REDIS_MODULES /var/opt/redislabs/lib/modules

RUN mkdir -p $REDIS_MODULES/ /var/opt/redislabs/artifacts
RUN chown -R redis:redis /var/opt/redislabs

COPY --from=builder --chown=redis:redis /build/redisgears.so $REDIS_MODULES/
COPY --from=builder --chown=redis:redis /build/bin/linux-x64-release/python3_* /var/opt/redislabs/modules/rg/python3/

# This is needed in order to allow extraction of artifacts from platform-specific build
# There is no use in removing this directory if $PACK !=1, because image side will only
#   increase if `docker build --squash` if not used.
# COPY --from=builder /build/artifacts/VERSION /var/opt/redislabs/artifacts/VERSION
# COPY --from=builder /build/artifacts/snapshot/ /var/opt/redislabs/artifacts/snapshot
COPY --from=builder /build/artifacts/ /var/opt/redislabs/artifacts

RUN	set -e ;\
	cd /var/opt/redislabs/modules/rg/ ;\
	ln -s python3 python3_`cat /var/opt/redislabs/artifacts/VERSION`

RUN if [ ! -z $(command -v apt-get) ]; then apt-get -qq update; apt-get -q install -y git; fi
RUN if [ ! -z $(command -v yum) ]; then yum install -y git; fi

CMD ["--loadmodule", "/var/opt/redislabs/lib/modules/redisgears.so"]
