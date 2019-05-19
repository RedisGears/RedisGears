# RedisGears Module
Dynamic execution framework for your Redis data:

## Docker

To quickly tryout RedisGears, launch an instance using docker:

```sh
docker run redislabs/redisgears:latest
```

## Build
### Prerequisites

* Install [redis 5.0](https://redis.io/) on you machine.
* On Mac OSX (High Sierra), install Xcode command line tools:

```bash
xcode-select --install
```

* Run: `make setup`


Install:

* Linux (Ubuntu 18.4): You'll need to `sudo apt install build-essential autotools-dev autoconf libtool`
* OSX (High Sierra): You'll need XCode and also `brew install autoconf libtool`

### Compile
```bash
sudo mkdir -p /opt/redislabs/lib
sudo chmod 755 /opt/redislabs/lib
make fetch # this will aquire git submodules
make all
```
Notice that part of the compilation is to create the gears virtual environment under `/opt/redislabs/lib/modules/python3/`

## Run
If you running gears on the same machine on which it was compile then its enough just loading the RedisGears module:

`--loamodule <path to redisgears.so`

If you run RedisGears on another machine, some extra setup is needed. For Gears to run properly its need to have the virtual environment which was created when it was compiled. All the required files are located in `/opt/redislabs/lib/modules/python3` make sure to put this directory on the same machine where Redis running and at the same path (i.e `/opt/redislabs/lib/modules/python3`).

## Tests
Tests are written in python using the [RLTest](https://github.com/RedisLabsModules/RLTest) library.
```
$ pip install git+https://github.com/RedisLabsModules/RLTest.git@master # optional, use virtualenv
$ make test
```

## Client libraries

Currently there is no special RedisGears client, that said, any client that allows sending custom commands to redis should be enough.

## Cluster Support
RedisGears support all of the operations on oss and enterprise cluster. Notice that the module needs to be loaded on all the cluster nodes. In addition, on oss cluster, after setting up the cluster you need to run `rs.refreshcluster` on each node.
