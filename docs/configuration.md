# RedisGears Configuration
RedisGears provides configuration options to control its operation. These options can be set when the module is bootstrapped and, in some cases, at runtime.

The following sections describe the configuration options and how to set them.

**Bootstrap Configuration**

You can set your configuration options when the module is loaded. The options are passed as a list of option names and their respective values. Configuration is supported both when using the `loadmodule` configuration directive as well as via the [Redis `MODULE LOAD` command](https://redis.io/commands/module-load).

!!! example "Example: Setting configuration options"
    To set the module's configuration options from the command line, run:

    ```
    redis-server --loadmodule /path/to/redisgears.so <option> <value> ...
    ```

    To set the module's configuration options in a Redis configuration file, use the following format:

    ```
    loadmodule /path/to/redisgears.so <option> <value> ...
    ```

    To set the module's configuration with the [`MODULE LOAD`](https://redis.io/commands/module-load) command, run:

    ```
    127.0.0.1:6379> MODULE LOAD /path/to/redisgears.so <option> <value> ...
    ```

**Runtime Configuration**

You may set certain configuration options at runtime. Refer to each option's description for runtime configuration.

!!! abstract "Related commands"
    The following RedisGears commands are related to configuration:

    * [`RG.CONFIGGET`](commands.md#rgconfigget)
    * [`RG.CONFIGSET`](commands.md#rgconfigset)

## MaxExecutions
The **MaxExecutions** configuration option controls the maximum number of executions that will be saved in the executions list. Once this threshold is reached, older executions will be deleted from the list in order of their creation (FIFO). Only executions that have finished (e.g. the 'done' or 'aborted' [status](functions.md#execution-status)) will be deleted.

_Expected Value_

Integer

_Default Value_

"1000"

_Runtime Configurability_

Supported.

!!! note
    Changing this option will affect the creation of new executions only.

**Examples**

```
127.0.0.1:6379> RG.CONFIGSET MaxExecutions 10
OK
```

## MaxExecutionsPerRegistration
The **MaxExecutionsPerRegistration** configuration option controls the maximum number of executions that are saved in the list per registration. Once this threshold is reached, older executions for that registration will be deleted from the list in order of their creation (FIFO). Only executions that have finished (e.g. the 'done' or 'aborted' [status](functions.md#execution-status)) will be deleted.

_Expected Value_

Integer

_Default Value_

"100"

_Runtime Configurability_

Supported.

!!! note
    Changing this option will affect the creation of new executions only.

**Examples**

```
$ 127.0.0.1:6379> RG.CONFIGSET MaxExecutionsPerRegistration 10
OK
```

## ProfileExecutions
The **ProfileExecutions** configuration option determines whether executions are profiled.

!!! important "Profiling impacts performance"
    Profiling requires reading the server's clock, which is a costly operation in terms of performance. Execution profiling is recommended only for debugging and should be disabled in production.

_Expected Value_

0 (disabled) or 1 (enabled)

_Default Value_

"0"

_Runtime Configurability_

Supported

## PythonAttemptTraceback
The **PythonAttemptTraceback** configuration option controls whether the engine tries producing stack traces for Python runtime errors.

_Expected Value_

0 (disabled) or 1 (enabled)

_Default Value_

"1"

_Runtime Configurability_

Supported

## OverridePythonAllocators
The **OverridePythonAllocators** configuration option controls whether RedisGears will override the default python memory allocators. Disabling this option, causes the python interpreter to increase performance (in some cases we saw improvement of up to 50%), the disadvantage is that the output of [RG.PYSTATS](commands.md#rgpystats) becomes invalid, and Redis will be unable to track and report memory usage by the python interpreter.

!!! important "Notice"
    Available as of v1.2.4

_Expected Value_

0 (disabled) or 1 (enabled)

_Default Value_

"1"

_Runtime Configurability_

Not Supported

## DownloadDeps
The **DownloadDeps** configuration option determines whether RedisGears will attempt to download missing Python dependencies.

_Expected Value_

0 (disabled) or 1 (enabled)

_Default Value_

"1"

_Runtime Configurability_

Not Supported

## DependenciesUrl
The **DependenciesUrl** configuration option sets the location from which RedisGears tries to download its Python dependencies.

_Expected Value_

URL-like string

_Default Value_

The default value is specific to the RedisGears version.

_Runtime Configurability_

Not Supported

## DependenciesSha256
The **DependenciesSha256** configuration option specifies the SHA265 hash value of the Python dependencies. This value is verified after the dependencies have been downloaded and will stop the server's startup in case of a mismatch.

_Expected Value_

String

_Default Value_

The default value is specific to the RedisGears version.

_Runtime Configurability_

Not Supported

## PythonInstallationDir
The **PythonInstallationDir** configuration option specifies the path for RedisGears' Python dependencies.

_Expected Value_

String

_Default Value_

/var/opt/redislabs/modules/rg

_Runtime Configurability_

Not Supported

## CreateVenv
The **CreateVenv** configuration option controls whether the engine will create a virtual Python environment.

_Expected Value_

0 (disabled) or 1 (enabled)

_Default Value_

"0"

_Runtime Configurability_

Not Supported

## ExecutionThreads
The **ExecutionThreads** configuration option sets the number of threads for executions.

_Expected Value_

Any integer greater than 0

_Default Value_

"3"

_Runtime Configurability_

Not Supported

## ExecutionMaxIdleTime
The **ExecutionMaxIdleTime** configuration option sets the maximal amount of idle time (in milliseconds) before execution is aborted. Idle time means no progress is made by the execution. The main reason for idle time is an execution that's blocked on waiting for records from another shard that had failed (i.e., crashed). In that case, the execution will be aborted after the specified time limit. The idle timer is reset once the execution starts progressing again.

_Expected Value_

Any integer greater than 0

_Default Value_

"5 seconds"

_Runtime Configurability_

Supported

## PythonInstallReqMaxIdleTime
The **PythonInstallReqMaxIdleTime** configuration option controls the maximal amount of idle time (in milliseconds) before Python's requirements installation is aborted. Idle time means that the installation makes no progress. The main reason for idle time is the same as for **ExecutionMaxIdleTime**.

_Expected Value_

Any integer greater than 0

_Default Value_

30000

_Runtime Configurability_

Supported

## SendMsgRetries
The **SendMsgRetries** configuration option controls the maximum number of retries for sending a message between RedisGears' shards. When a message is sent and the shard disconnects before acknowledging it, or when it returns an error, the message will be resent until this threshold is met. Setting the value to 0 means unlimited retries.

_Expected Value_

Any integer greater or eqaul 0

_Default Value_

3

_Runtime Configurability_

Supported

## Plugin
The **Plugin** configuration option allows to specify which RedisGears plugin you want to load. Usually a RedisGears plugin is the same as language support. Currently RedisGears supports python and java (jvm languages). The **Plugin** configuration options tells RedisGears which language plugins to load.

_Expected Value_

String

_Default Value_

No default

_Runtime Configurability_

Not Supported

