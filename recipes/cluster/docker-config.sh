BIN_PATH=/usr/local/bin
CLUSTER_HOST=127.0.0.1
CONTAINER_IP=`hostname -I`
PORT=30000
NODES=3
REPLICAS=0
PROTECTED_MODE=no
ADDITIONAL_OPTIONS="--cluster-announce-ip $CONTAINER_IP --loadmodule /var/opt/redislabs/lib/modules/redisgears.so"