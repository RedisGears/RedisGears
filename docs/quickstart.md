# RedisGears Module
Dynamic execution framework for your Redis data.

## Docker
To quickly tryout RedisGears, launch an instance using docker:

```sh
docker run -p 6379:6379 redislabs/redisgears:latest
```

## Build

### Prerequisites
* Install [Redis 5.0](https://redis.io/) on your machine.
* On Mac OSX (High Sierra), install Xcode command line tools:

```bash
xcode-select --install
```

* Run: `make setup`

### Compile
```bash
sudo mkdir -p /opt
make fetch # this will aquire git submodules
make all
```
Notice that part of the compilation is to create the gears virtual environment under `/var/opt/redislabs/lib/modules/python3/`

## Run
If you run gears on the same machine on which it was compiled, then it's enough to load the RedisGears module:

`--loamodule <path to redisgears.so>`

If you run RedisGears on another machine, some extra setup is needed. For RedisGears to run properly, it needs to have the virtual environment which was created when it was compiled. All the required files are located in `/var/opt/redislabs/lib/modules/python3`. Make sure to copy this directory to the machine where Redis is running and to the same path (i.e `/opt/redislabs/lib/modules/python3`).

## Tests
Tests are written in python using the [RLTest](https://github.com/RedisLabsModules/RLTest) library.
```
$ make test
```

## Client libraries
Gears python library can be found [here](https://github.com/RedisGears/redisgears-py).
In addition, any client that allows sending custom commands to Redis should be enough.

## Cluster Support
All of RedisGears' operations are fully supported on OSS and Enterprise clusters. Note that the module needs to be loaded on all the cluster nodes. In addition, on OSS cluster, after setting up the cluster you need to run `RG.REFRESHCLUSTER` on each node.
