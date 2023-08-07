---
title: "Stream Triggers"
linkTitle: "Stream Triggers"
weight: 2
description: >
    Execute a JavaScript function when an item is added to a stream
---

Redis Stack's triggers and functions feature comes with a full stream API to processes data from [Redis Stream](https://redis.io/docs/manual/data-types/streams/) Unlike RedisGears v1 that provided a micro batching API, the new triggers and functions feature provides a **real streaming** API, which means that the data will be processed as soon as it enters the stream.

## Register a Stream consumer

Triggers and functions provide an API that allows to register a stream trigger. Do not get confused with [Redis Streams Consumer groups](https://redis.io/docs/manual/data-types/streams/#consumer-groups), triggers and functions uses the Redis Module API to efficiently read the stream and manage its consumers. This approach gives a much better performance as there is no need to invoke any Redis commands in order to read from the stream. Lets see a simple example:

```js
#!js api_version=1.0 name=myFirstLibrary

redis.registerStreamTrigger(
    "consumer", // consumer name
    "stream", // streams prefix
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

Argument Description:

* consumer - the consumer name.
* stream - streams name prefix on which to trigger the callback.
* callback - the callback to invoke on each element in the stream. Following the same rules of [Sync and Async invocation](/docs/interact/programmability/triggers-and-functions/concepts/sync_async/). The callback will be invoke only on primary shard.

If we register this library (see the [quick start](/docs/interact/programmability/triggers-and-functions/quick_start/) section to learn how to Register a RedisGears function) and run the following command on our Redis:

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
    "stream_name_raw": "<stream name as ArrayBuffer>",
    "record":[
        ["<key>", "<value>"],
        .
        .
        ["<key>", "<value>"]
    ],
    "record_raw":[
        ["<key_raw>", "<value_raw>"],
        .
        .
        ["<key_raw>", "<value_raw>"]
    ],
}
```

The reason why the record is a list of touples and not an object is because the Redis Stream specifications allows duplicate keys.

Notice that `stream_name` and `record` fields might contains `null`'s if the data can not be decoded as string. the `*_raw` fields will always be provided and will contains the data as `JS` `ArrayBuffer`.

We can observe the streams which are tracked by our registered consumer using `TFUNCTION LIST` command:

```
127.0.0.1:6379> TFUNCTION LIST LIBRARY lib vvv
1)  1) "engine"
    1) "js"
    2) "api_version"
    3) "1.0"
    4) "name"
    5) "lib"
    6) "pending_jobs"
    7) (integer) 0
    8) "user"
    9)  "default"
    10) "functions"
   1)  (empty array)
   2)  "stream_triggers"
   3)  1)  1) "name"
           1) "consumer"
           2) "prefix"
           3) "stream"
           4) "window"
           5) (integer) 1
           6) "trim"
           7) "disabled"
           8) "num_streams"
          1)  (integer) 2
          2)  "streams"
          3)  1)  1) "name"
                  1) "stream:2"
                  2) "last_processed_time"
                  3) (integer) 0
                  4) "avg_processed_time"
                  5) "0"
                  6) "last_lag"
                  7) (integer) 0
                  8) "avg_lag"
                 1)  "0"
                 2)  "total_record_processed"
                 3)  (integer) 2
                 4)  "id_to_read_from"
                 5)  "1657030412715-0"
                 6)  "last_error"
                 7)  "None"
                 17) "pending_ids"
                 18) (empty array)
              1)  1) "name"
                  1) "stream:1"
                  2) "last_processed_time"
                  3) (integer) 1
                  4) "avg_processed_time"
                  5) "0.5"
                  6) "last_lag"
                  7) (integer) 1
                  8) "avg_lag"
                 1)  "0.5"
                 2)  "total_record_processed"
                 3)  (integer) 2
                 4)  "id_to_read_from"
                 5)  "1657030405323-0"
                 6)  "last_error"
                 7)  "None"
                 8)  "pending_ids"
                 9)  (empty array)
   4)  "keyspace_triggers"
   5)  (empty array)
```

## Enable Trimming and Set Window

We can enable stream trimming by adding `isStreamTrimmed` optional argument after the trigger callback, we can also set the `window` argument that controls how many elements can be processed simultaneously. example:

```js
#!js api_version=1.0 name=myFirstLibrary

redis.registerStreamTrigger(
    "consumer", // consumer name
    "stream", // streams prefix
    function(c, data) {
        // callback to run on each element added to the stream
        redis.log(JSON.stringify(data, (key, value) =>
            typeof value === 'bigint'
                ? value.toString()
                : value // return everything else unchanged
        ));
    }, 
    {
        isStreamTrimmed: true,
        window: 3   
    }
);

```

The default values are:
* `isStreamTrimmed` - `false`
* `window` - 1

It is enough that a single consumer will enable trimming so that the stream will be trimmed. The stream will be trim according to the slowest consumer that consume the stream at a given time (even if this is not the consumer that enabled the trimming). Raising exception during the callback invocation will **not prevent the trimming**. The callback should decide how to handle failures by invoke a retry or write some error log. The error will be added to the `last_error` field on `TFUNCTION LIST` command.

## Data processing Guarantees

As long as the primary shard is up and running we guarantee exactly once property (the callback will be triggered exactly one time on each element in the stream). In case of failure such as shard crashing, we guarantee at least once property (the callback will be triggered at least one time on each element in the stream)

## Upgrades

When upgrading the consumer code (using the `REPLACE` option of `TFUNCTION LOAD` command) the following consumer parameters can be updated:

* Window
* Trimming

Any attempt to update any other parameter will result in an error when loading the library.
