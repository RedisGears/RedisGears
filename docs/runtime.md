# RedisGears Functions Runtime

Python RedisGears functions are run using an embedded Python interpreter. Each function uses a separate sub-interpreter. All functions share the same environment and dependencies. The environment is imported with several defaults.

The following sections describe the runtime environment.

## Python Interpreter
RedisGears embeds a Python version 3.7.2+ interpreter.

All functions use that interpreter. Each function has its own sub-interpreter that isolates its execution context from other functions. This means each function has its own globals.

!!! info "Further reference"
    For more information refer to:

      * [Sub-interpreters design](subinterpreters.md)

## Environment
The interpreter's environment can be extended with any dependent package that can later be imported and used by functions in their respective sub-interpreters.

!!! info "Further reference"
    For more information about installing dependencies refer to:

      * [The `REQUIREMENTS` subcommand of `RG.PYEXECUTE`](commands.md#rgpyexecute)
      * [The QuickStart's Python Virtual Environment section](quickstart.md#python-virtual-environment)

## GearsBuilder
The `GearsBuilder` class is imported to the runtime's environment by default.

It exposes the functionality of the function's [context builder](functions.md#context-builder).

## Execute
The `execute()` function is imported to the runtime's environment by default.

This function executes an arbitrary Redis command.

!!! info "Further reference"
    For more information about Redis commands refer to:

      * [Redis commands](https://redis.io/commands)

**Python API**
```python
def execute(command, *args)
```

_Arguments_

* _command_: the command to execute
* _args_: the command's arguments

**Examples**
```python
# Pings the server (should return 'PONG')
reply = execute('PING')
```

## ConfigGet
The `ConfigGet()` function is imported to the runtime's environment by default.

This function fetches the current value of a RedisGears [configuration](#configuration.md) option.

**Python API**
```python
def ConfigGet(key)
```

_Arguments_

* _key_: the configuration option key

**Examples**
```python
# Gets the current value for 'ProfileExecutions'
foo = ConfigGet('ProfileExecutions')
```

## GearsConfigGet
The `GearsConfigGet()` function is imported to the runtime's environment by default.

This function fetches the current value of a RedisGears [configuration](configuration.md) option and returns a default value if that key does not exist.

**Python API**
```python
def GearsConfigGet(key, default=None)
```

_Arguments_

* _key_: the configuration option key
* _default_: a default value

**Examples**
```python
# Gets the 'foo' configuration option key and defaults to 'bar'
foo = GearsConfigGet('foo', default='bar')
```

## Hashtag
The `hashtag()` function is imported to the runtime's environment by default.

This function returns a hashtag that maps to the lowest hash slot served by the local engine's shard. Put differently, it is useful as a hashtag for partitioning in a cluster.

**Python API**
```python
def hashtag()
```

**Examples**
```python
# Get the shard's hashtag
ht = hashtag()
```

## Log
The `Log()` function is imported to the runtime's environment by default.

This function prints a message to Redis' log.

**Python API**
```python
def Log(level='notice', message)
```
? this isn't pythonic :/

_Arguments_

* _level_: the message's log level can be one of these:
    * **'debug'**
    * **'verbose'**
    * **'notice'**
    * **'warning'**
* _message_: the message to output

**Examples**
```python
# Dumps every datum in the DB to the log for "debug" purposes
GB().foreach(lambda x: Log('debug', str(x))).run()
```
