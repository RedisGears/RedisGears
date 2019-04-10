FROM redis:latest AS builder

RUN set -ex; \
    apt-get update; \
	apt-get install -y python git

ADD . /redisgears
WORKDIR /redisgears

RUN python system-setup.py
# RUN make clean DEPS=1 ALL=1
RUN make get_deps
# PYTHON_ENCODING=ucs4
RUN make all SHOW=1

# Set up the runner
FROM redis:latest
ENV REDIS_MODULES /opt/redislabs/lib/modules

RUN mkdir -p $REDIS_MODULES/

COPY --from=builder /redisgears/redisgears.so $REDIS_MODULES/
COPY --from=builder /redisgears/artifacts/release/redisgears-dependencies.*.tgz /tmp/

RUN tar xzf /tmp/redisgears-dependencies.tgz -C /

CMD ["--loadmodule", "/opt/redislabs/lib/modules/redisgears.so", "PythonHomeDir", "/opt/redislabs/lib/modules/python27"]
