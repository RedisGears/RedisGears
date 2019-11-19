# BUILD redisfab/redisgears-${OSNICK}:M.m.b-${ARCH}

# OSNICK=bionic|stretch|buster
ARG OSNICK=buster

# OS=debian:buster-slim|debian:stretch-slim|ubuntu:bionic
ARG OS=debian:buster-slim

# ARCH=x64|arm64v8|arm32v7
ARG ARCH=x64

ARG REDISVER=5.0.5

#----------------------------------------------------------------------------------------------
# FROM redis:latest AS redis
FROM redisfab/redis-${ARCH}-${OSNICK}:${REDISVER} AS redis
FROM ${OS} AS builder

ARG OSNICK
ARG OS
ARG ARCH
ARG REDISVER

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
RUN if [ "$TEST" = "1" ]; then make test; fi

#----------------------------------------------------------------------------------------------
# FROM redis:latest
FROM redisfab/redis-${ARCH}-${OSNICK}:${REDISVER}

ARG OSNICK
ARG OS
ARG ARCH
ARG REDISVER

ENV REDIS_MODULES /opt/redislabs/lib/modules

RUN mkdir -p $REDIS_MODULES/

COPY --from=builder /build/redisgears.so $REDIS_MODULES/
COPY --from=builder /build/artifacts/release/redisgears-dependencies.*.tgz /tmp/

RUN tar xzf /tmp/redisgears-dependencies.*.tgz -C /

CMD ["--loadmodule", "/opt/redislabs/lib/modules/redisgears.so", "PythonHomeDir", "/opt/redislabs/lib/modules/python3"]
