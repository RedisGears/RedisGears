# RedisGears Functions

A RedisGears **function** is a formal description of the processing steps in the data flow.

```
                                      +------------+
                                      | Function   |
                    +-------------+   | +--------+ |
                    | Input data  +-->+ | Reader | |
                    +-------------+   | +---+----+ |
                                      |     v      |
                                      | +---+----+ |
                                      | | Step 1 | |
                                      | +---+----+ |
                                      |     |      |
                                      |    ...     |
                                      |     v      |
                                      | +---+----+ |
                                      | | Step n | |
                                      | +---+----+ |
                                      |     v      |
                    +-------------+   | +---+----+ |
                    | Results     +<--+ | Action | |
                    +-------------+   | +--------+ |
                                      +------------+
```

A function always:

  1. Starts with a [**reader**](readers.md)
  2. Operates on zero or more [**records**](overview.md#record)
  3. Consists of zero or more [**operations**](operations.md) (a.k.a steps)
  4. Ends with an [**action**](overview.md#action)
  5. Returns zero or more [**results**](overview.md#result)
  6. May generate zero or more errors

## Execution
A function is executed by the RedisGears engine in one of two ways:

  1. **Batch**: execution is immediate and on existing data
  2. **Event**: execution is triggered by new events and on their data

The function's mode of execution is determined by its action. There are two types of actions:

  1. [**Run**](#run): runs the function in batch
  2. [**Register**](#register): registers the function to be triggered by events

When executed, whether as batch or event, the function's context is managed by the engine. Besides the function's logic, the context also includes its breakdown to internal execution steps, state, status, statistics, results and any errors encountered among other things.

!!! abstract "Related commands"
    The following RedisGears commands are related to executing functions:

    * [`RG.PYEXECUTE`](commands.md#rgpyexecute)
    * [`RG.ABORTEXECUTION`](commands.md#rgabortexecution)
    * [`RG.DUMPEXECUTIONS`](commands.md#rgdumpexecutions)
    * [`RG.GETEXECUTION`](commands.md#rggetexecution)

## Execution ID
The execution of every function is internally assigned with a unique value called **Execution ID**.

The ID is string value made of two parts that are delimited by a hyphen ('-'), as follows:

  1. **Shard ID**: the 40-bytes-long identifier of a [shard](overview.md#shard) in a [cluster](overview.md#cluster)
  2. **Sequence**: an ever-increasing counter

!!! example "Example: Execution IDs"
    When used in _stand-alone_ mode, the Shard ID is set to zeros ('0') so the first execution ID would be:

    ```
    0000000000000000000000000000000000000000-1
    ```

    Whereas in _cluster_ mode it could be:

    ```
    a007297351b007297351c007297351d007297351-1
    ```

## Registration
The representation of an event-driven function is called a registration.

Registrations are persisted in Redis' snapshots, a.k.a RDB files. This allows recovering both data and events handlers in the database in the event of failure.

!!! abstract "Related commands"
    The following RedisGears commands are related to registering functions:

    * [`RG.PYEXECUTE`](commands.md#rgpyexecute)
    * [`RG.DUMPREGISTRATIONS`](commands.md#rgdumpregistrations)
    * [`RG.UNREGISTER`](commands.md#rgunregister)

## Registration ID
Every registration has a unique internal identifier. It is generated in the same manner as the [Execution ID](#execution-id).

## Context Builder
RedisGears functions in Python always begin with a context builder - the [`#!python class GearsBuilder`](runtime.md#gearsbuilder).

!!! tip
    `GB()` is an alias for `GearsBuilder()`.

    It is intended to be used for brevity, increased productivity and the reduction of finger strain due to repetitive typing.

**Python API**
```python
class GearsBuilder(reader='KeysReader', defaultArg='*', desc=None)
```
_Arguments_

* _reader_: the function's [reader](readers.md)
* _defaultArg_: An optional argument that the reader may need. These are usually a key's name, prefix, glob-like or a regular expression. Its use depends on the function's reader type and action.
* _desc_: an optional description

**Examples**
```python
# Here's the default context builder being run
GearsBuilder().run()

# You can also do this
gb = GB()
gb.register()
```

## Actions
An action is special type of operation. It is always the function's final step.

### Run
The **Run** action runs a function as batch. The function is executed once and exits once data is exhausted by its reader.

Trying to run more than one function in the same execution will fail with an error.

!!! example "Multiple executions error"
    ```
    127.0.0.1:30001> RG.PYEXECUTE "GB().run()\nGB().run()"
    (error) [... 'spam.error: Can not run more then 1 executions in a single script']
    ```

!!! important "Execution is always asynchronous"
    Batch functions are **always executed asynchronously** by the RedisGears engine. That means means that they are run in a background thread rather than by the main process of the Redis server.

**Python API**
```python
class GearsBuilder.run(arg=None, convertToStr=True, collect=True)
```

_Arguments_

* _arg_: An optional argument that's passed to the reader as its _defaultArg_. It means the following:
    * A glob-like pattern for the [KeysReader](readers.md#keysreader) and [KeysOnlyReader](readers.md#keysonlyreaders) readers
    * A key name for the [StreamReader](readers.md#streamreader) reader
* _convertToStr_: when `True` adds a [map](operations.md#map) operation to the flow's end that stringifies records
* _collect_: when `True` adds a [collect](operations.md#collect) operation to flow's end

**Examples**
```python
# Runs the function
GB() \
  .run()
```

### Register
The **Register** action registers a function as an event handler. The function is executed each time an event arrives. Each time it is executed, the function operates on the event's data and once done is suspended until its future invocations by new events.

**Python API**
```python
class GearsBuilder.register(regex='*', mode='async', batch=1, duration=0,
  eventTypes=None, keyTypes=None, onRegistered=None, onFailedPolicy="continue",
  onFailedRetryInterval=1)
```

_Arguments_

* _regex_: An optional argument that's passed to the reader as its _defaultArg_. It means the following:
    * A key prefix for the [KeysReader](readers.md#keysreader) and [KeysOnlyReader](readers.md#keysonlyreaders) readers
    * A key name for the [StreamReader](readers.md#streamreader) reader
* _mode_: the execution mode of the triggered function. Can be one of:
    * **'async'**: execution will be asynchronous across the entire cluster
    * **'async_local'**: execution will be asynchronous and restricted to the handling shard
    * **'sync'**: execution will be synchronous and local
* _batch_: the batch size that triggers execution for the [StreamReader](readers.md#streamreader) reader
* _duration_: the interval between executions for the [StreamReader](readers.md#streamreader) reader (takes precedence over the _batch_ argument)
* _eventTypes_: A whitelist of event types that trigger for the [KeysReader](readers.md#keysreader) or [KeysOnlyReader](readers.md#keysonlyreaders) readers. The list may contain one or more:
    * Any Redis or Redis module command?
    * Events ...?
* _keyTypes_: A whitelist of key types that trigger execution for the [KeysReader](readers.md#keysreader) or [KeysOnlyReader](readers.md#keysonlyreaders) readers. The list may contain one or more from the following:
    * Redis core types: 'string', 'hash', 'list', 'set', 'zset' or 'stream'
    * Any module data type?
* _onRegistered_: A function [callback](operations.md#callback) that's called on each shard upon function registration. It is a good place to initialize non-serializable objects such as network connections.
* _onFailedPolicy_: A policy for handling failures of the function when using the [StreamReader](readers.md#streamreader) reader. It can be set to one of the following:
    * **'continue'**: ignores a failure and continues to the next execution
    * **'abort'**: stops further executions
    * **'retry'**: retries the execution after an interval specified with _onFailedRetryInterval_
* _onFailedRetryInterval_: the interval in seconds between retries of failed executions of a function that uses the [StreamReader](readers.md#streamreader) reader

**Examples**
```python
# Registers the function
GB() \
  .register()
```

## Results
The execution of a function yields zero or more **result** records. The result is made up of any records output by the function's last operation and just before its final action.

Results are stored within the function's execution context.

!!! abstract "Related commands"
    The following RedisGears commands are related to getting results:

    * [`RG.GETRESULTS`](commands.md#rggetresults)
    * [`RG.GETRESULTSBLOCKING`](commands.md#rggetresultsblocking)
