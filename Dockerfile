# BUILD redisfab/redisgears-${ARCH}-${OSNICK}:M.m.b

# stretch|bionic
ARG OSNICK=stretch

#----------------------------------------------------------------------------------------------
FROM redislabs/redis-${OSNICK}:5.0.5 AS builder

ADD . /build
WORKDIR /build

RUN ./deps/readies/bin/getpy2
RUN python system-setup.py
RUN make get_deps
RUN make all SHOW=1

#----------------------------------------------------------------------------------------------
FROM redislabs/redis-${OSNICK}:5.0.5

ENV REDIS_MODULES /opt/redislabs/lib/modules

RUN mkdir -p $REDIS_MODULES/

COPY --from=builder /build/redisgears.so $REDIS_MODULES/
COPY --from=builder /build/artifacts/release/redisgears-dependencies.*.tgz /tmp/

RUN tar xzf /tmp/redisgears-dependencies.*.tgz -C /

CMD ["--loadmodule", "/opt/redislabs/lib/modules/redisgears.so", "PythonHomeDir", "/opt/redislabs/lib/modules/python3"]
