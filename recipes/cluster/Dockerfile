FROM redislabs/redisgears:latest

WORKDIR /cluster
COPY create-cluster .
COPY docker-config.sh .
COPY docker-entrypoint.sh /usr/local/bin
EXPOSE 30001-30006
CMD []

