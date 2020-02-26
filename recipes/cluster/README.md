# RedisGears Cluster Docker Image
Use this to have a RedisGears-enabled Redis Cluster running inside a single container with one command. This uses Redis' [_create-cluster_](https://github.com/antirez/redis/tree/unstable/utils/create-cluster) utility.

By default, it bootstraps a 3-shard cluster on ports 30001-30003. To run:

```
docker run -d -p 30001:30001 -p 30002:30002 -p 30003:30003 redislabs/rgcluster
```

## Configuration
The default configuration is set by the contents of [docker-config.sh](docker-config.sh). To override it, create your own - e.g., "myconfig.sh" - on the host, for example and mount it to the container's `/cluster/config.sh` filesystem like so:

```
docker run -d $PWD/myconfig.sh:/cluster/config.sh redislabs/rgcluster
```
