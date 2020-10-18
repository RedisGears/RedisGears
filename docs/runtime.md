# RedisGears Functions Runtime

Python RedisGears functions are run using an embedded Python interpreter. Each function uses a separate sub-interpreter. All functions share the same environment and dependencies. The environment is imported with several defaults.

The following sections describe the runtime environment.

## Python Interpreter
RedisGears embeds a Python version 3.7.2+ interpreter.

All functions use that interpreter. Each call to `RG.PYEXECUTE` maintains its own globals dictionary that isolates its execution context from other calls. This means that all the functions submitted in that call share the interpreter and  globals dictionary.

!!! info "Further reference"
    For more information refer to:

      * [Isolation Technics design](isolation_technics.md)

## Environment
The interpreter's environment can be extended with any dependent package that can later be imported and used by functions in their respective sub-interpreters.

!!! info "Further reference"
    For more information about installing dependencies refer to:

      * [The `REQUIREMENTS` argument of `RG.PYEXECUTE`](commands.md#rgpyexecute)
      * [The QuickStart's Python Virtual Environment section](quickstart.md#python-virtual-environment)

## GearsBuilder
The `GearsBuilder` class is imported to the runtime's environment by default.

It exposes the functionality of the function's [context builder](functions.md#context-builder).

## execute
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
{{ include('runtime/execute.py') }}
```

## atomic
The `atomic()` Python context is imported to the runtime's environment by default.

The context ensures that all operations in it are executed atomically by blocking the main Redis process.

**Python API**

```python
class atomic()
```

**Examples**

```python
{{ include('runtime/atomic.py') }}
```

## configGet
The `configGet()` function is imported to the runtime's environment by default.

This function fetches the current value of a RedisGears [configuration](#configuration.md) option.

**Python API**

```python
def configGet(key)
```

_Arguments_

* _key_: the configuration option key

**Examples**

```python
{{ include('runtime/configget.py') }}
```

## gearsConfigGet
The `gearsConfigGet()` function is imported to the runtime's environment by default.

This function fetches the current value of a RedisGears [configuration](configuration.md) option and returns a default value if that key does not exist.

**Python API**

```python
def gearsConfigGet(key, default=None)
```

_Arguments_

* _key_: the configuration option key
* _default_: a default value

**Examples**

```python
{{ include('runtime/gearsconfigget.py') }}
```

## hashtag
The `hashtag()` function is imported to the runtime's environment by default.

This function returns a hashtag that maps to the lowest hash slot served by the local engine's shard. Put differently, it is useful as a hashtag for partitioning in a cluster.

**Python API**

```python
def hashtag()
```

**Examples**

```python
{{ include('runtime/hashtag.py') }}
```

## log
The `log()` function is imported to the runtime's environment by default.

This function prints a message to Redis' log.

**Python API**

```python
def log(message, level='notice')
```

_Arguments_

* _message_: the message to output
* _level_: the message's log level can be one of these:
    * **'debug'**
    * **'verbose'**
    * **'notice'**
    * **'warning'**

**Examples**

```python
{{ include('runtime/log.py') }}
```

## gearsFuture
The `gearsFuture()` function is imported to the runtime's environment by default.

This function returns a `gearsFuture` object, which allows another thread/process to process the record. Returning this object from a step's operation tells RedisGears to suspend execution until background processing had finished/failed.

The `gearsFuture` object provides two control methods: `continueRun()` and `continueFailed()`. Both methods are thread-safe and can be called at any time to signal that the background processing has finished. `continueRun` signals success and its argument is a record for the main process. `continueFailed` reports a failure to the main process and its argument is a string describing the failure.

Calling `gearsFuture()` is supported only from the context of the following operations:
* `map`
* `flatmap`
* `filter`
* `foreach`
* `aggregate`
* `aggregateby`

Any attempt to create a `gearsFuture` outside of those steps will results in exception.

**Examples**

```python
{{ include('runtime/gearsFuture.py') }}
```

### gearsFuture with Python Async Await
`gearsFuture` is also supported seamlessly with python async await, so it possible to do the following:

```python
{{ include('runtime/async.py') }}
```
