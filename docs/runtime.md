# RedisGears Functions Runtime

Python RedisGears functions are run using an embedded Python interpreter. Each function uses a separate sub-interpreter. All functions share the same environment and dependencies. The environment is imported with several defaults.

The following sections describe the runtime environment.

## Python Interpreter
RedisGears embeds a Python version 3.7.2+ interpreter.

All functions use this interpreter. Each call to `RG.PYEXECUTE` maintains its own globals dictionary that isolates its execution context from other calls. This means that all of the functions submitted in a given call share the same interpreter and globals dictionary.

!!! info "Further reference"
    For more information refer to:

      * [Isolation Techniques design](isolation.md)

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

## Async Await

For a full explination about async await look at [Async Await Support](intro.md#async-await-support)

### createFuture
The `createFuture` function is imported to the runtime's environment by default.

This function returns a `gearsFuture` object, which can be waited on using `await` inside a coroutine.

**Python API**

```python
def createFuture()
```

### setFutureResults
The `setFutureResults` function is imported to the runtime's environment by default.

This function allows set a result on a future object returned from [createFuture](runtime#createFuture)

**Python API**

```python
def setFutureResults(f, res)
```

_Arguments_

* f: future object to set the result on.
* res: the result to set on the future object.

### setFutureException
The `setFutureException` function is imported to the runtime's environment by default.

This function allows set an exception on a future object returned from [createFuture](runtime#createFuture)

**Python API**

```python
def setFutureException(f, ex)
```

_Arguments_

* f: future object to set the result on.
* ex: the exception to set on the future object.

### runCoroutine
The `runCoroutine` function is imported to the runtime's environment by default.

This function allows run a coroutine in a dedicated event loop.

**Python API**

```python
def runCoroutine(cr, f=None, delay=0)
```

_Arguments_

* cr: coroutine to run.
* f: future object to set the coroutine result on (if none the result are ignored).
* delay: delay (in seconds) to start the coroutine.

### isAsyncAllow
The `isAsyncAllow` function is imported to the runtime's environment by default.

This function allows to know if async await can be used in the current execution, for more info refer to [sync with multi exec](async_await_advance_topics.md#sync-with-multi-exec).

**Python API**

```python
def isAsyncAllow()
```

## call_next
The `call_next` function is imported to the runtime's environment by default.
 
This function allows you to call the next hook registered on the command or the original Redis command (for further reading about command hook, please refer to [Commands Hook](commands_hook.md)). It is only possible to call this API when hooking a command. Any attempt to call this API in the wrong context will result in an error.
 
**Python API**
 
```python
def call_next(*args)
```
 
_Arguments_
 
* args: arguments with which to invoke the next hook.
 
## override_reply
The `override_reply` function is imported to the runtime's environment by default.
This function allows to override the reply of the client linked to the execution. It is only possible to use it on [`KeysReader`](readers.md#keysreader) with combination of the `commands` argument. For further reading please refer to [key miss event](miss_event.md) taturial.

**Python API**

```python
def override_reply(reply)
```

_Arguments_

* reply: the new reply to send to the client. If the reply starts with `-` it will be returned as error reply. If the reply starts with `+` it will be returned as a status reply.

## flat_error
The `flat_error` function is imported to the runtime's environment by default.
This function raise a special exception type that instructs gears not to extract the error stack trace.

**Python API**

```python
def flat_error(msg)
```

_Arguments_

* msg: The error msg to add as is in the execution error list.

