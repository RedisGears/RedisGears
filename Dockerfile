FROM redis:5.0.3 AS builder

# Set up a build environment
RUN set -ex; \
    apt-get update; \
	apt-get install -y python git

ADD . /redisgears
WORKDIR /redisgears

RUN python system-setup.py
RUN make clean
RUN make get_deps PYTHON_ENCODING=ucs4
RUN make
RUN make pyenv
RUN make pack

# Set up the runner
FROM redis:5.0.3
ENV RUNTIME_DEPS "python"
ENV LD_LIBRARY_PATH /usr/lib/redis/modules

RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends $RUNTIME_DEPS;

RUN set -ex;\
    mkdir -p "$LD_LIBRARY_PATH/deps";

COPY --from=builder /redisgears/redisgears.so "$LD_LIBRARY_PATH"
COPY --from=builder /redisgears/src/deps/cpython "$LD_LIBRARY_PATH/deps/cpython/"

CMD ["--loadmodule", "/usr/lib/redis/modules/redisgears.so"]
