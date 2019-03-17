FROM redis:5.0.3 AS builder

ENV BUILD_DEPS "build-essential autotools-dev autoconf automake libtool python git ca-certificates lsb-release xxd"

# Set up a build environment
RUN set -ex;\
    apt-get update;\
    apt-get install -y --no-install-recommends $BUILD_DEPS;

ADD . /redisgears
WORKDIR /redisgears
RUN make get_deps
RUN make WITHPYTHON=1

# Set up the runner
FROM redis:5.0.3
ENV RUNTIME_DEPS "python"
ENV LD_LIBRARY_PATH /usr/lib/redis/modules

RUN set -ex;\
    apt-get update;\
    apt-get install -y --no-install-recommends $RUNTIME_DEPS;

RUN ln -fs /usr/lib/python2.7/plat-x86_64-linux-gnu/_sysconfigdata.py /usr/lib/python2.7/
RUN ln -fs /usr/lib/python2.7/plat-x86_64-linux-gnu/_sysconfigdata_nd.py /usr/lib/python2.7/

RUN set -ex;\
    mkdir -p "$LD_LIBRARY_PATH/deps";

COPY --from=builder /redisgears/redisgears.so "$LD_LIBRARY_PATH"
COPY --from=builder /redisgears/src/deps/cpython "$LD_LIBRARY_PATH/deps/cpython/"

CMD ["--loadmodule", "/usr/lib/redis/modules/redisgears.so", "PythonHomeDir", "/usr/lib/redis/modules/deps/cpython/"]
