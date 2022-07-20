# Configuraiton

RedisGears provides configuration options to control its operation. These options can be set when the module is bootstrapped and, in some cases, at runtime.

The following sections describe the configuration options and how to set them.

## Bootstrap Configuration

You can set your configuration options when the module is loaded. 
When the module is loaded on start time, the module configuration can be set on the Redis configuration file itself. When loading the module on runtime the configuration can be given to the [MODULE LOADEX](https://redis.io/commands/module-loadex/) command. Each configuration must be prefixed with the module name, `redisgears_2.<configuration name>`.

## Runtime Configuration

You may set certain configuration options at runtime. Setting a configuration at runtime is done using [CONFIG SET](https://redis.io/commands/config-set/) command. Here also, Each configuration must be prefixed with the module name, `redisgears_2.<configuration name>`.

Example:

```bash
> config set redisgears_2.lock-redis-timeout 1000
OK
```

# Configurations

## execution-threads

The `execution-threads` configuration option controls the amount of background thread that runs JS code. **Notice that libraries are considered single threaded**, This configuration allows parallelise the invocation of multiple libraries only.

_Expected Value_

Integer

_Default_

1

_Minumum Value_

1

_Maximum Value_

32

_Runtime Configurability_

No

## library-fatal-failure-policy

The `library-fatal-failure-policy` configuration option controls how to handle a fatal error. Fatal error is consider one of the following:

* Block timedout - The function blocks the Redis processes for to long (configurable using [lock-redis-timeout](#lock-redis-timeout) configuraion value)
* OOM - The function consumer to much memory (configurable using [library-maxmemory](#library-maxmemory) configuraion value).

This configuration basically allow chosing between 2 options:

* Do not break atomocity property, even at the cost of killing the Redis processes.
* Keep my Redis processes alive, even at the cost of losing atomicity.

_Expected Value_

* kill - Save the atomicity property. Risk of killing the Redis processes.
* abort - Abort the invocation of the function and keep the Redis processes alive. Risk of losing the atomicity property.

_Default_

abort

_Runtime Configurability_

Yes

## library-maxmemory

The `library-maxmemory` configuration option controls the maximum amount of memory a single library is allowed to consume. Exceeding this limit is considered a fatal error and will be handled base of the [library-fatal-failure-policy](#library-fatal-failure-policy) configuration value.

_Expected Value_

Integer

_Default_

1G

_Minumum Value_

16M

_Maximum Value_

2G

_Runtime Configurability_

No

## lock-redis-timeout

The `lock-redis-timeout` configuration option controls the maximum amount of time (in MS) a library can lock Redis. Exceeding this limit is considered a fatal error and will be handled base of the [library-fatal-failure-policy](#library-fatal-failure-policy) configuration value.

_Expected Value_

Integer

_Default_

500 MS

_Minumum Value_

100 MS

_Maximum Value_

Unlimited

_Runtime Configurability_

Yes