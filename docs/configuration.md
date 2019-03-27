# Run-time configuration

RedisGears supports a few run-time configuration options that should be determined when loading the module. In time more options will be added.

## Passing Configuration Options During Loading

In general, passing configuration options is done by appending arguments after the `--loadmodule` argument in the command line, `loadmodule` configuration directive in a Redis config file, or the `MODULE LOAD` command. For example:

In redis.conf:

```
loadmodule redisgears.so OPT1 OPT2
```

From redis-cli:

```
127.0.0.6379> MODULE load redisgears.so OPT1 OPT2
```

From command line:

```
$ redis-server --loadmodule ./redisgears.so OPT1 OPT2
```

*Notice that currently its not possible to change configuration options at run time, but its planned to be supported soon.*

## RedisGears configuration options

## PythonHomeDir

Tells the python interpreter where to look for the default python libraries

### Default

**[compiled dir]**/src/cpython/

### Example

```
$ redis-server --loadmodule ./redisearch.so PythonHomeDir /home/user/cpython/
```

---

## MaxExecutions

The maximum amount of execution to save. When reach this number, old execution will be deleted in a FIFO order.

### Default

1000

### Example

```
$ redis-server --loadmodule ./redisearch.so MaxExecutions 10
```

---
