# RedisGears Module
Dynamic execution framework for your Redis data:

## Docker

To quickly tryout RedisGears, launch an instance using docker:

```sh
docker run redislabs/redisgears:latest
```

## Build
### Prerequisites
Install [redis 5.0](https://redis.io/) on you machine.

Install:

* Linux (Ubuntu 18.4): Yo'll need to `sudo apt install build-essential autotools-dev autoconf libtool`
* OSX (High Sierra): You'll need XCode and also `brew install autoconf libtool`

### Compile
```bash
git submodule init
git submodule update
make get_deps
make
```
It is possible to run without python support by just running `make`.

## Run
If you running gears on the same machine on which it was compile then its enough just loading the RedisGears module:

`--loamodule <path to redisgears.so`

If you run RedisGears on another machine, some extra setup is needed. For Gears to run properly its need to have the cpython directory it was compiled with. All the required files are located in `/src/deps/cpython/` make sure to put this directory on the same machine where Redis running and use `PythonHomeDir` config variable to tell RedisGears where it should look for this cpython directory:

`--loamodule <path to redisgears.so> PythonHomeDir <path to cpython direcotry>`

## Tests
Tests are written in python using the [RLTest](https://github.com/RedisLabsModules/RLTest) library.
```
$ pip install git+https://github.com/RedisLabsModules/RLTest.git@master # optional, use virtualenv
$ cd pytest
$ ./run_tests.sh
```

## Client libraries

todo!!!

## Cluster Support
RedisGears support all of the operations on oss and enterprise cluster. Notice that the module needs to be loaded on all the cluster nodes. In addition, on oss cluster, after setting up the cluster you need to run `rs.refreshcluster` on each node.
