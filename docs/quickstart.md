# RedisGears Quickstart
RedisGears is a [Redis](glossary.md#redis) [module](glossary.md#module). To run it you'll need a Redis server (v5 or greater) and the module's shared library.

The following sections describe how to get started with RedisGears.

## Docker
The quickest way to try RedisGears is by launching its official Docker container image:

```
docker run -p 6379:6379 redislabs/redisgears:latest
```

A Redis Cluster with RedisGears variant is also available:

```sh
docker run -p 30001:30001 -p 30002:30002 -p 30003:30003 redislabs/rgcluster:latest
```

!!! info "Further reference"
    Refer to the [cluster's README file](https://github.com/RedisGears/RedisGears/blob/master/recipes/cluster/README.md) for information on configuring the Dockerized cluster container.

## Building

### Prerequisites
* Install [Redis 5.0](https://redis.io/) on your machine.
* On macOS install Xcode command line tools:

```
xcode-select --install
```

* Run: `make setup`

### Compiling
To compile the module do the following:

```
sudo mkdir -p /var/opt/redislabs
make fetch
make all
```

!!! important "The /var/opt/redislabs/lib/modules directory"
    The compilation process creates a virtual Python environment and places the binaries at this path: `/var/opt/redislabs/lib/modules`

## Loading
To load the module on the same server is was compiled on simply use the `--loadmodule` command line switch, the `loadmodule` configuration directive or the [Redis `MODULE LOAD` command](https://redis.io/commands/module-load) with the path to module's library.

In case you've compiled the module on a different server than the one loading it, copy the contents of the '/var/opt/redislabs/lib/modules` to the server.

## Testing
Tests are written in Python and the [RLTest](https://github.com/RedisLabsModules/RLTest) library.

To run the tests after installing the dependencies use:

```
make test
```
