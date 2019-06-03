ARG OS=ubuntu:bionic
ARG OSNICK=bionic
ARG ARCH=x64

#----------------------------------------------------------------------------------------------
FROM redislabs/redis-base-${ARCH}-${OSNICK}:latest AS builder

ADD . /build
WORKDIR /build

RUN ./deps/readies/bin/getpy2
RUN python system-setup.py
RUN make get_deps
RUN make all SHOW=1 PYTHON_ENCODING=ucs4

RUN echo /usr/local/lib/python3.7/site-packages > /opt/redislabs/lib/modules/python3/.venv/lib/python3.7/site-packages/local.pth

#----------------------------------------------------------------------------------------------
FROM redislabs/redis-base-${ARCH}-${OSNICK}:latest

ENV REDIS_MODULES /opt/redislabs/lib/modules

RUN mkdir -p $REDIS_MODULES/

COPY --from=builder /build/redisgears.so $REDIS_MODULES/
COPY --from=builder /build/artifacts/release/redisgears-dependencies.*.tgz /tmp/

RUN tar xzf /tmp/redisgears-dependencies.*.tgz -C /

CMD ["--loadmodule", "/opt/redislabs/lib/modules/redisgears.so", "PythonHomeDir", "/opt/redislabs/lib/modules/python3"]
