# BUILD redisfab/redisgears-${ARCH}-${OSNICK}:M.m.b

# stretch|bionic
ARG OSNICK=stretch

#----------------------------------------------------------------------------------------------
# FROM redisfab/redis-${OSNICK}:5.0.5 AS builder
FROM redis:latest AS builder

ADD . /build
WORKDIR /build

RUN ./deps/readies/bin/getpy2
RUN python ./system-setup.py 
RUN make fetch SHOW=1
RUN echo nproc=$(nproc); echo NPROC=$(eval "$X_NPROC"); make all SHOW=1 -j $(eval "$X_NPROC")

#----------------------------------------------------------------------------------------------
# FROM redisfab/redis-${OSNICK}:5.0.5
FROM redis:latest

ENV REDIS_MODULES /opt/redislabs/lib/modules

RUN mkdir -p $REDIS_MODULES/

COPY --from=builder /build/redisgears.so $REDIS_MODULES/
COPY --from=builder /build/artifacts/release/redisgears-dependencies.*.tgz /tmp/

RUN tar xzf /tmp/redisgears-dependencies.*.tgz -C /

CMD ["--loadmodule", "/opt/redislabs/lib/modules/redisgears.so", "PythonHomeDir", "/opt/redislabs/lib/modules/python3"]
