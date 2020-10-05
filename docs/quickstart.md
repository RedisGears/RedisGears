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

## Download

A pre-compiled version can be downloaded from [RedisLabs download center](https://redislabs.com/download-center/modules/).

**Notice** in order to support running Python based recipies on enviorment without internet connection you should also download the relevant `Redisgears Dependencies`.

## Building

### Prerequisites
* Install git
for Debian like systems:
```
apt-get install git
```
for Fedora like systems:
```
yum install git
```


* Install the build-essential package, or its equivalent, on your system:
for Debian-like systems:
```
apt-get install build-essential
```
for Fedora-like systems:
```
yum install devtoolset-7
scl enable devtoolset-7 bash
```

* Install [Redis 6.0.1 or higher](https://redis.io/) on your machine.

```
git clone https://github.com/antirez/redis.git
cd redis
git checkout 6.0.1 
make
make install
```

* On macOS install Xcode command line tools:

```
xcode-select --install
```

### Clone
To get the code and its submodules do the following:
```
git clone https://github.com/RedisGears/RedisGears.git
cd RedisGears
git submodule update --init --recursive
```

### Compiling
Inside the RedisGears directory run the following:

```
./deps/readies/bin/getpy2
make setup # might require root privileges for installations
make fetch
make all
```

You will find the compiled binary under `bin/linux-x64-release/redisgears.so` with a symbol link to it on the main directory (called `redisgears.so`).

## Loading
To load the module on the same server it was compiled on simply use the `--loadmodule` command line switch, the `loadmodule` configuration directive or the [Redis `MODULE LOAD` command](https://redis.io/commands/module-load) with the path to module's library.

For example to load the module to local Redis after you followed [Building](#building) steps run:
```
redis-server --loadmodule ./redisgears.io
```

If you compiled the module on a different server than the one loading it (or downloaded the module), copy the directory `bin/linux-x64-release/python3_<version>` (where `<version>` is the version compiled) to the target server. Use the [PythonInstallationDir](configuration.md#pythoninstallationdir) configuration parameter to specify the path of the copied directory on the target to RedisGears. **Important:** the directory's name, that is `python3_<version>` should not be changed.

## Testing
Tests are written in Python and the [RLTest](https://github.com/RedisLabsModules/RLTest) library.

To run the tests after installing the dependencies use:

```
make test
```
