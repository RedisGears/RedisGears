# RedisGears Readers
The **reader** is the mandatory first step of any [RedisGears function](functions.md), and every function has exactly one reader. A reader reads data and generates input [records](glossary.md#record) from it. The input records are consumed by the function.

A function's reader is declared when initializing its `#!python class GearsBuilder` [context builder](functions.md#context-builder).

RedisGears supports several types of readers that operate on different types of input data. Furthermore, each reader may be used to process [batch](glossary.md#batch-processing) [streaming](glossary.md#event-processing) data.

| Reader | Output | Batch | Event |
| --- | --- | --- | --- |
| [KeysReader](#keysreader) | Redis keys and values | Yes | Yes |
| [KeysOnlyReader](#keysonlyreader) | Redis keys | Yes | No |
| [StreamReader](#streamreader) | Redis Stream messages | Yes | Yes |
| [PythonReader](#pythonreader) | Arbitrary | Yes | No |
| [ShardsIDReader](#shardsidreader) | Shard ID | Yes | No |
| [CommandReader](#commandreader) | Command arguments | No | Yes |

The following sections describe the different readers' operation.

## KeysReader
The **KeysReader** scans the Redis database.

It generates records from keys and their respective values.

**Input**

The reader scans the entire database and any keys that are found can be used as input for generating records.

**Output**

A record is output for each input key. The record is a dictionary structure that has four keys and their respective values:

  * **'key'**: the name of the key
  * **'value'**: the value of the key (`#!python None` if the deleted)
  * **'type'**: the core Redis type may be: 'string', 'hash', 'list', 'set', 'zset' or 'stream'
  * **'event'**: the event that triggered the execution (`#!python None` if the execution was created via the [run](functions.md#run) function)

**Batch Mode**

The reader scans the entire database for keys. For each key found, it first reads a key's name, then fetches its value (unless used with `#!python readValue=False` argument) and finally generates a record.

Its operation can be controlled by the following means:

  * Glob-like pattern: generates records only for key names that match the pattern
  * Read value: a Boolean specifying whether the value is read or not
  * Use scanning: a Boolean specifying whether to scan and match the pattern or use it as an explicit key name

**Event Mode**

The reader is executed in response to events that are generated from write operations in the Redis database.

Its operation can be controlled with the following means:

  * Prefix: generates records only for key names that start with the prefix
  * Events: same, but only for whitelisted events
  * Types: same, but only for whitelisted data types
  * Read value: a Boolean specifying whether the value is read or not

**Python API**

**_Batch Mode_**

```python
class GearsBuilder('KeysReader', defaultArg='*').run(noScan=False, readValue=True)
```

_Arguments_

* _defaultArg_: a glob-like pattern of key names
* _noScan_: when `#!python True` the pattern is used as an explicit key name
* _readValue_: when `#!python False` the value will not be read, so the **'type'** and **'value'** of the record will be set to `#!python None`

**_Event Mode_**

```python
class GearsBuilder('KeysReader').register(prefix='*', eventTypes=None,
  keyTypes=None, readValue=True)
```

_Arguments_

* _prefix_: a prefix of key names
* _eventTypes_: a whitelist of event types that trigger execution when the [KeysReader](readers.md#keysreader) are used. The list may contain one or more:
    * Any Redis or module command
    * Any [Redis event](https://redis.io/topics/notifications)
* _keyTypes_: a whitelist of key types that trigger execution when using the [KeysReader](readers.md#keysreader) or [KeysOnlyReader](readers.md#keysonlyreaders) readers. The list may contain one or more from the following:
    * Redis core types: 'string', 'hash', 'list', 'set', 'zset' or 'stream'
    * Redis module types: 'module'
* _readValue_: when `#!python False` the value will not be read, so the **'type'** and **'value'** of the record will be set to `#!python None`

_Return_

The output record's type is a `#!python dict`.

The value is cast from the Redis type as follows:

| Redis type | Python type |
| --- | --- |
| string | `#!python str` |
| hash | `#!python dict` |
| list | `#!python list` |
| set | `#!python None` |
| zset | `#!python None` |
| stream | `#!python None` |
| module | `#!python None` |

**Examples**

**_Batch Mode_**

```python
{{ include('readers/keysreader-run.py') }}
```

**_Event Mode_**

```python
{{ include('readers/keysreader-register.py') }}
```

## KeysOnlyReader
The **KeysOnlyReader** is implemented as a [PythonReader](#pythonreader) and therefore does not support the [`register()` action](functions.md#register). It returns keys' names as string records.

**Input**

The reader scans the entire database and any keys that are found can be used as input for generating records.

**Output**

A record is output for each input key. The record is a simple string that is the key's name.

**Batch Mode**

The reader scans the entire database for keys. For each key found, it returns the key as a string record.

Its operation can be controlled by the following means:

  * Glob-like pattern: generates records only for key names that match the pattern
  * Scan count: the amount of effort each scan iteration will invest
  * Use scanning: a Boolean specifying whether to scan and match the pattern or use it as an explicit key name
  * Shard-specific pattern: customizes the pattern for each shard

**Event Mode**

Not supported.

**Python API**

**_Batch Mode_**

```python
class GearsBuilder('KeysOnlyReader').run(pattern='*', count=1000, noScan=False, patternGenerator=None)
```
* _pattern_: pattern of keys to return
* _count_: the `COUNT` argument to the [Redis `SCAN` command](https://redis.io/commands/scan)
* _noScan_: when `#!python True` the pattern is used as an explicit key name
* _patternGenerator_: a callback that returns a shard-specific pattern. If provided it overrides the _pattern_ and _noScan_ arguments. The callback will be called by each shard and is expected to return a tuple with two items, the first being the _pattern_ string and the other the _noScan_ value.

## StreamReader
The **StreamReader** reads the messages from one or more [Redis Streams](glossary.md#stream) and generates records from these.

**Input**

The reader reads messages from the Stream value of a Redis key.

**Output**

A record is output for each message in the input Stream. The record is a dictionary structure with the following key:

  * **key**: the Stream key name as string
  * **id**: the message's id in the Stream
  * **value**: a Python dictionary containing the message's data

All field-value pairs in the message's data are included as key-value pairs in the record under the _value_ key.

**Batch Mode**

The reader reads the Stream from the beginning to the last message in it. Each message generates a record.

Its operation can be controlled with the following:

  * Key name: the name of the key storing the Stream
  * Start ID: message ID from which to start reading messages

**Event Mode**

The reader is executed in response to events generated by new messages added to the stream.

Its operation can be controlled with the following:

  * Key name or prefix: the name or prefix of keys that store Streams
  * Batch size: the number of new messages that trigger execution
  * Duration: the time to wait before execution is triggered, regardless of the batch size
  * Failure policy: the policy for handling execution failures. May be one of:
    * **'continue'**: ignores a failure and continues to the next execution. This is the default policy.
    * **'abort'**: stops further executions.
    * **'retry'**: retries the execution after an interval specified with onFailedRetryInterval (default is one second).
  * Trimming: whether or not to trim the stream

**Python API**

**_Batch Mode_**

```python
class GearsBuilder('StreamReader', defaultArg='*').run(fromId='0-0')
```

_Arguments_

* _defaultArg_: the name or prefix of keys that store Streams
* _fromId_: message id from which to start read messages

**_Event Mode_**

```python
class GearsBuilder('StreamReader').run(prefix='*', batch=1, duration=0, onFailedPolicy='continue', onFailedRetryInterval=1, trimStream=True)
```

_Arguments_

* _prefix_: the name or prefix of keys that store Streams
* _batch_: the number of new messages that trigger execution
* _duration_: the time to wait before execution is triggered, regardless of the batch size (0 for no duration)
* _onFailedPolicy_: the policy for handling execution failures, values should be as describe above
* _onFailedRetryInterval_: the interval (in milliseconds) in which to retry in case _onFailedPolicy_ is **'retry'**
* _trimStream_: when `#!python True` the stream will be trimmed after execution

_Return_

The output record's type is a `#!python dict` with fields and values as explained above.

## PythonReader
The reader is executed with a function callback that is a [Python generator](https://wiki.python.org/moin/Generators).

**Input**

A generator function callback.

**Output**

Anything `yield`ed by the generator function callback.

**Batch Mode**

The reader iterates the generator's yielded records.

**Event Mode**

Not supported.

**Python API**

**_Batch Mode_**

```python
class GearsBuilder('PythonReader').run(generator)
```

_Arguments_

* _generator_: the [generator](https://wiki.python.org/moin/Generators) function callback

**Examples**

The following example shows how to use the reader with a simple generator:

```python
{{ include('readers/pythonreader-run-001.py') }}
```

In cases where the generator needs additional input arguments, use a function callback that returns a generator function like so:

```python
{{ include('readers/pythonreader-run-002.py') }}
```

## ShardsIDReader
The reader returns a single record that is the shard's cluster identifier.

!!! info "RedisGears Trivia"
    The 'ShardsIDReader' is implemented using the 'PythonReader' reader and this generator callback:

    ```python
    def ShardReaderCallback():
    res = execute('RG.INFOCLUSTER')
    if res == 'no cluster mode':
        yield '1'
    else:
        yield res[1]
    ```

**Output**

The shard's cluster identifier.

**Batch Mode**

The reader returns a single record that is the shard's identifier.

**Event Mode**

Not supported.

**Python API**

**_Batch Mode_**

```python
class GearsBuilder('ShardsIDReader').run()
```

_Arguments_

None.

**Examples**

```python
{{ include('readers/shardidreader-run.py') }}
```

## CommandReader
The **CommandReader** reads the trigger name and arguments sent by an [`RG.TRIGGER` command](commands.md#rgtrigger).

**Input**

The arguments of the [`RG.TRIGGER` command](commands.md#rgtrigger).

**Output**

The trigger's name and arguments.

**Batch Mode**

Not supported.

**Event Mode**

The reader returns a list record with elements being the trigger's name followed by any arguments that were provided.

**Python API**

**_Event Mode_**

```python
class GearsBuilder('CommandReader').register(trigger=None)
```

_Arguments_

* _trigger_: the trigger's name

**Examples**

First, call [`RG.PYEXECUTE`](commands.md#rgpyexecute) with the following:

```python
{{ include('readers/commandreader-register.py') }}
```

Then, you can do this:

```
redis> RG.TRIGGER CountVonCount bat bat
1) "CountVonCount got 2 arguments! Ah ah ah!"
```
