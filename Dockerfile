FROM redis:latest AS builder

RUN set -ex; \
    apt-get update; \
	apt-get install -y python git

ADD . /redisgears
WORKDIR /redisgears

RUN python system-setup.py
RUN make get_deps
RUN make all SHOW=1 PYTHON_ENCODING=ucs4

# Set up the runner
FROM redis:latest
ENV REDIS_MODULES /opt/redislabs/lib/modules

RUN mkdir -p $REDIS_MODULES/

COPY --from=builder /redisgears/redisgears.so $REDIS_MODULES/
COPY --from=builder /redisgears/artifacts/release/redisgears-dependencies.*.tgz /tmp/

RUN tar xzf /tmp/redisgears-dependencies.*.tgz -C /

CMD ["--loadmodule", "/opt/redislabs/lib/modules/redisgears.so", "PythonHomeDir", "/opt/redislabs/lib/modules/python3"]
