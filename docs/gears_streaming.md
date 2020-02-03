# RedisGears Streaming
Gears also support a streaming api, i.e processing a streaming data. It is recommended to use the redis stream datatype combine with redisgears to achieve a full streaming processing solution.

## Streaming processing

The idea behind streaming process is processing the data at the moment it arrives. Gears give the ability to register a gears execution on events and trigger execution when ever such event occurs. Currently gears supports two event types:

* keys changes event - trigger when ever a key is touched
* redis stream event - trigger when a new value is added to the stream

It is possible to register an execution on an event using the `register` function (instead of using the `run` function).  For example, the following Gears will be triggered each time a key changes.  The execution will delete the key, basically it will deny the ability to put anything in redis.
```
GearsBuilder().foreach(lambda x: execute('del', x['key'])).register()
```

It is also possible to register on streams using the following syntax:
```
GearsBuilder('StreamReader').foreach(lambda x: execute('set', x['streamId'], str(x))).register('s1')
```
This execution will be triggered for each new record that enters stream `s1`.

The format of the records that arrive from the stream reader is:
```
{'streamId': '<stream-id>', <field1>:<value1>, <field2>:<value2>, ...}
```

### KeysReader Register Arguments
```
def register(self, regex='*', mode='async', eventTypes=None, keyTypes=None, OnRegistered=None)
```
* regex - The prefix of keys on which to register
* mode:
    * sync - execution will be trigger on the same thread of the key space even (use it when you want to make sure execution was finished before the reply was returned to the user)
    * async - execution will be trigger on an async thread
    * async_local - execution will be triggered on an async thread but will be local to the currect shards only
* eventTypes - List of commands on which to trigger event. When set it is treated as white list.
* keyTypes - List of keys types on which to trigger event. When set it is treated as white list.
* OnRegistered - A callback to be execute on registration, will be trigger on each shard. It is recommended to use it to initialize a none serializable objects like network connections.

### StreamReader Register Arguments
```
def register(self, regex='*', mode='async', batch=1, duration=0, onRegistered=None, onFailedPolicy="retry", onFailedRetryInterval=1):
```
* regex - The prefix of keys on which to register
* mode:
    * sync - execution will be trigger on the same thread of the key space even (use it when you want to make sure execution was finished before the reply was returned to the user)
    * async - execution will be trigger on an async thread
    * async_local - execution will be triggered on an async thread but will be local to the currect shards only
* batch - The size of the batch (number of records to read from stream) for each trigged execution. 
* duration - Duration in miliseconds to trigger an execution even if batch size was not reached
* onRegistered - A callback to be execute on registration, will be trigger on each shard. It is recommended to use it to initialize a none serializable objects like network connections.
* onFailedPolicy - Tells RedisGears what to do on failure
    * retry - wait for a given interval and then retry (interval is provided by 'onFailedRetryInterval')
    * abort - stop processing events
    * continue - ignore the error and continue processing events
* onFailedRetryInterval - Relevant only when 'onFailedPolicy' set to 'retry', set the retry interval (in seconds).