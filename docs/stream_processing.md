# Stream Processing with RedisGears 2.0

RedisGears 2.0 comes with a full stream API to processes data from [Redis Stream](https://redis.io/docs/manual/data-types/streams/) Unlike RedisGears v1 that provided a micro batching API, RedisGears 2.0 provides a **real streaming** API, which means that the data will be processed as soon as it enters the stream.

## Register a Stream consumer

RedisGears provide an API that allows Register a stream consumer. Do not get confuse with [Redis Streams Consumer groups](https://redis.io/docs/manual/data-types/streams/#consumer-groups), RedisGears uses Redis Module API to efficiently read the stream and manage its consumers. This approach gives a much better performance as there is no need to invoke any Redis commands in order to read from the stream. Lets see a simple example:

```js
#!js name=lib

redis.register_stream_consumer(
    "consumer", // consumer name
    "stream", // streams prefix
    1, // window
    false, // trim stream
    function(c, data) {
        // callback to run on each element added to the stream
        redis.log(JSON.stringify(data, (key, value) =>
            typeof value === 'bigint'
                ? value.toString()
                : value // return everything else unchanged
        ));
    }
);
```

Argument Discription:

* consumer - the consumer name.
* stream - streams name prefix on which to trigger the callback.
* window - how many elements can be proceesed simultaneously.
* trim stream - whether or not to trim the stream.
* callback - the callback to invoke on each element in the stream. Following the same rules of [Sync and Async invocation](sync_and_async_run.md). The callback will be invoke only on primary shard.

If we register this library (see the [getting started](../README.md) section to learn how to Register a RedisGears function) and run the following command on our Redis:

```
XADD stream:1 * foo1 bar1
XADD stream:1 * foo2 bar2
XADD stream:2 * foo1 bar1
XADD stream:2 * foo2 bar2
```

We will see the following line on the Redis log file:

```
2630021:M 05 Jul 2022 17:13:22.506 * <redisgears_2> {"id":["1657030402506","0"],"stream_name":"stream:1","record":[["foo1","bar1"]]}
2630021:M 05 Jul 2022 17:13:25.323 * <redisgears_2> {"id":["1657030405323","0"],"stream_name":"stream:1","record":[["foo2","bar2"]]}
2630021:M 05 Jul 2022 17:13:29.475 * <redisgears_2> {"id":["1657030409475","0"],"stream_name":"stream:2","record":[["foo1","bar1"]]}
2630021:M 05 Jul 2022 17:13:32.715 * <redisgears_2> {"id":["1657030412715","0"],"stream_name":"stream:2","record":[["foo2","bar2"]]}
```

The `data` argument which pass to the stream consumer callback are in the following format:

```json
{
    "id": ["<ms>", "<seq>"],
    "stream_name": "<stream name>",
    "record":[
        ["<key>", "<value>"],
        .
        .
        ["<key>", "<value>"]
    ]
}
```

The reason why the record is a list of touples and not an object is because the Redis Stream spacifications allows duplicate keys.

We can observe the streams which are tracked by our registered consumer using [RG.FUNCTION LIST](commands.md#rgfunction-list) command:

```
127.0.0.1:6379> RG.FUNCTION LIST LIBRARY lib vvv
1)  1) "engine"
    2) "js"
    3) "name"
    4) "lib"
    5) "pending_jobs"
    6) (integer) 0
    7) "user"
    8) "default"
    9) "functions"
   10) (empty array)
   11) "stream_consumers"
   12) 1)  1) "name"
           2) "consumer"
           3) "prefix"
           4) "stream"
           5) "window"
           6) (integer) 1
           7) "trim"
           8) "disabled"
           9) "num_streams"
          10) (integer) 2
          11) "streams"
          12) 1)  1) "name"
                  2) "stream:2"
                  3) "last_processed_time"
                  4) (integer) 0
                  5) "avg_processed_time"
                  6) "0"
                  7) "last_lag"
                  8) (integer) 0
                  9) "avg_lag"
                 10) "0"
                 11) "total_record_processed"
                 12) (integer) 2
                 13) "id_to_read_from"
                 14) "1657030412715-0"
                 15) "last_error"
                 16) "None"
                 17) "pending_ids"
                 18) (empty array)
              2)  1) "name"
                  2) "stream:1"
                  3) "last_processed_time"
                  4) (integer) 1
                  5) "avg_processed_time"
                  6) "0.5"
                  7) "last_lag"
                  8) (integer) 1
                  9) "avg_lag"
                 10) "0.5"
                 11) "total_record_processed"
                 12) (integer) 2
                 13) "id_to_read_from"
                 14) "1657030405323-0"
                 15) "last_error"
                 16) "None"
                 17) "pending_ids"
                 18) (empty array)
   13) "notifications_consumers"
   14) (empty array)
   15) "gears_box_info"
   16) (nil)

```

## Enable Trimming

It is enough that a single consumer will enable trimming so that the stream will be trimmed. The stream will be trim according to the slowest consumer that consume the stream at a given time (even if this is not the consumer that enabled the trimming). Raising exception durring the callback invocation will **not prevent the trimming**. The callback should decide how to handle failures by invoke a retry or write some error log. The error will be added to the `last_error` field on [RG.FUNCTION LIST](commands.md#rgfunction-list) command.

## Data processing Guarantees

As long as the primary shard is up and running we guarantee exactly once property (the callback will be triggered exactly one time on each element in the stream). In case of failure such as shard crashing, we guarantee at least once propert (the callback will be triggered at least one time on each element in the stream)

## Upgrades

When upgrading the consumer code (using the `UPGRADE` option of [`RG.FUNCTION LOAD`](commands.md#rgfunction-load) command) the following consumer parameters can be updated:

* Window
* Trimming

Any attempt to update any other parameter will result in an error when loading the library.