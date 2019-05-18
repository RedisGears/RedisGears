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

## Passing configuration options at runtime

It is possible to modify certain configuration parameters at runtime using `RG.CONFIGSET` command. The command received the configuration parameter name and its value, for example:
```
$ RG.CONFIGSET ProfileExecutions 1 # will enable execution profiler
```

## RedisGears configuration options

## PythonHomeDir

Tells the python interpreter where to look for the default python libraries

### Default

**[compiled dir]**/src/cpython/

### Configurable at Runitime

** No

### Example

```
$ redis-server --loadmodule ./redisearch.so PythonHomeDir /home/user/cpython/
```

---

## MaxExecutions

The maximum amount of execution to save. When reach this number, old execution will be deleted in a FIFO order.

### Default

1000

### Configurable at Runitime

** Yes

### Example

```
$ redis-server --loadmodule ./redisearch.so MaxExecutions 10
```

---

## ProfileExecutions

Controls whether the internal execution plan profiler is active.

Note: enabling the profiler impacts overall performance - use with judiciously and with caution.

Possible values:
* 0 - disabled
* not 0 - enabled

### Default

0 (disabled)

### Configurable at Runitime

** Yes

## PythonAttemptTraceback

Controls whether traceback is attempted onw Python errors.

Possible values:
* 0 - disabled
* not 0 - enabled

## Default

1 (enableds)

### Configurable at Runitime

** Yes