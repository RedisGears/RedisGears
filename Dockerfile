# BUILD redisfab/redisgears-${OSNICK}:M.m.b-${ARCH}

ARG REDIS_VER=5.0.8

# OSNICK=bionic|stretch|buster
ARG OSNICK=buster

# OS=debian:buster-slim|debian:stretch-slim|ubuntu:bionic
ARG OS=debian:buster-slim

# ARCH=x64|arm64v8|arm32v7
ARG ARCH=x64

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

ARG PACK=0
ARG TEST=0

RUN if [ "$PACK" = "1" ]; then make pack; fi
RUN if [ "$TEST" = "1" ]; then TEST= make test; fi

#----------------------------------------------------------------------------------------------
FROM redisfab/redis:${REDIS_VER}-${ARCH}-${OSNICK}

ARG OSNICK
ARG OS
ARG ARCH
ARG REDIS_VER
ARG PACK

ENV REDIS_MODULES /var/opt/redislabs/lib/modules

RUN mkdir -p $REDIS_MODULES/ /var/opt/redislabs/artifacts

COPY --from=builder /build/redisgears.so $REDIS_MODULES/
COPY --from=builder /build/artifacts/ /var/opt/redislabs/artifacts/

RUN tar --warning=no-timestamp  -xzf /var/opt/redislabs/artifacts/release/redisgears-dependencies.*.tgz -C /

RUN if [ "$PACK" != "1" ]; then rm -rf /var/opt/redislabs/artifacts; fi

RUN chown -R redis:redis /var/opt/redislabs

RUN if [ ! -z $(command -v apt-get) ]; then apt-get -qq update; apt-get -q install -y git; fi
RUN if [ ! -z $(command -v yum) ]; then yum install -y git; fi

CMD ["--loadmodule", "/var/opt/redislabs/lib/modules/redisgears.so"]
