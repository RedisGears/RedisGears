# RedisGears Configuration
RedisGears provides configuration options that control its operation. These options can be set when the module is bootstrapped and in some cases also during runtime.

The following sections describe the configuration options the means for setting them.

**Bootstrap Configuration**

Configuration options can be set when the module is loaded. The options are passed as a list of option names and their respective values. Configuration is supported both when using the `loadmodule` configuration directive as well as via the [Redis `MODULE LOAD` command](https://redis.io/commands/module-load).

!!! example "Example: Setting configuration options"
    For setting the module's configuration options from the command line use:

    ```
    redis-server --loadmodule /path/to/redisgears.so <option> <value> ...
    ```

    For setting the module's configuration options in with .conf file use the following format:

    ```
    loadmodule /path/to/redisgears.so <option> <value> ...
    ```

    For setting the module's configuration with the [`MODULE LOAD`](https://redis.io/commands/module-load) command use:

    ```
    127.0.0.1:6379> loadmodule /path/to/redisgears.so <option> <value> ...
    ```

**Runtime Configuration**

Some configuration options may be set at runtime. Refer to each option's description for runtime configurability.

!!! abstract "Related commands"
    The following RedisGears commands are related to configuration:

    * [`RG.CONFIGGET`](commands.md#rgconfigget)
    * [`RG.CONFIGSET`](commands.md#rgconfigset)

## MaxExecutions
The **MaxExecutions** configuration option controls the maximum number of executions that will be saved in the executions list. Once this threshold value is reached, older executions will be deleted from the list by order of their creation (FIFO). Only executions that had finished (e.g. the 'done' or 'aborted' [status](functions.md#execution-status)) are deleted.

_Expected Value_

Integer

_Default Value_

"1000"

_Runtime Configurability_

Supported.

!!! note
    Changing this option will impact the creation of new executions only.

**Examples**

```
127.0.0.1:6379> RG.CONFIGSET MaxExecutions 10
OK
```

## MaxExecutionsPerRegistration
The **MaxExecutionsPerRegistration** configuration option controls the maximum number of executions that are saved in the list per registration. Once this threshold value is reached, older executions for that registration will be deleted from the list by order of their creation (FIFO). Only executions that had finished (e.g. the 'done' or 'aborted' [status](functions.md#execution-status)) are deleted.

_Expected Value_

Integer

_Default Value_

"100"

_Runtime Configurability_

Supported.

!!! note
    Changing this option will impact the creation of new executions only.

**Examples**

```
$ 127.0.0.1:6379> RG.CONFIGSET MaxExecutionsPerRegistration 10
OK
```

## ProfileExecutions
The **ProfileExecutions** configuration option controls whether executions are profiled.

!!! important "Profiling impacts performance"
    Profiling requires reading the server's clock, which is a costly operation in terms of performance. Execution profiling is recommended only for debugging purposes and should be disabled in production.

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

## DownloadDeps
The **DownloadDeps** configuration option controls whether or not RedisGears will try to download the python dependencies if those are missing.

_Expected Value_

0 (disabled) or 1 (enabled)

_Default Value_

"1"

_Runtime Configurability_

Not Supported

## DependenciesUrl
The **DependenciesUrl** configuration option controls the location from which RedisGears tries to download its dependencies.

_Expected Value_

Url like string

_Default Value_

The default value matches the RedisGears version.

_Runtime Configurability_

Not Supported

## DependenciesSha256
The **DependenciesSha256** configuration option specify the sha265sum of the python dependencies. This sha will be verified after RedisGears download the python dependencies and if sha check failed RedisGears will failed the Redis startup process.

_Expected Value_

String

_Default Value_

The default value matches the RedisGears version.

_Runtime Configurability_

Not Supported

## pythonInstallationDir
The **pythonInstallationDir** configuration option controls where RedisGears will install/search for the python dependencies.

_Expected Value_

string

_Default Value_

?

_Runtime Configurability_

Not Supported

## CreateVenv
The **CreateVenv** configuration option controls whether the engine will create a virtual environment for the python run.

_Expected Value_

0 (disabled) or 1 (enabled)

_Default Value_

"0"

_Runtime Configurability_

Not Supported

## ExecutionThreads
The **ExecutionThreads** configuration option controls the number of threads that will run executions.

_Expected Value_

Any number greater than 1

_Default Value_

"3"

_Runtime Configurability_

Not Supported
