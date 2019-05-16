# RedisGears Streaming
Gears also support a streaming api, i.e processing a streaming data. It is recomended to use the redis stream datatype combine with redisgears to achieve a full streaming processing solution.

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
