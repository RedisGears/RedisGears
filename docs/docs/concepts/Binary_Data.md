---
title: "Binary data"
linkTitle: "Binary data"
weight: 8
description: >
    Working with binary data
---

By default, triggers and functions will decode all data as a string and will raise error on failures. Though useful for most users, sometimes there is a need to work with binary data. In order to do so, the library developer has to consider the following:

1. Binary function arguments
2. Binary command results
3. Binary key names on [KeySpace triggers](/docs/interact/programmability/triggers-and-functions/concepts/triggers/keyspace_triggers/)
4. Binary data on [stream triggers](/docs/interact/programmability/triggers-and-functions/concepts/triggers/stream_triggers/)

### Binary Function Arguments

It is possible to instruct triggers and functions not to decode function arguments as `JS` `Strings` using the [redis.functionFlags.RAW_ARGUMENTS](/docs/interact/programmability/triggers-and-functions/concepts/function_flags/) function flag. In this case, the function arguments will be given as `JS` `ArrayBuffer`. Example:

```js
#!js api_version=1.0 name=lib
redis.registerFunction("my_set",
    (c, key, val) => {
        return c.call("set", key, val);
    },
    {
        flags: [redis.functionFlags.RAW_ARGUMENTS]
    }
);
```

The above example will allow us to set `key` and `val` even if those are binary data. Run example:

```bash
127.0.0.1:6379> TFCALL lib.my_set 1 "\xaa" "\xaa"
"OK"
127.0.0.1:6379> get "\xaa"
"\xaa"
```

Notice that the `call` function also accepts `JS` `ArrayBuffer` arguments.

### Binary Command Results

Getting function arguments as binary data is not enough. We might want to read binary data from a Redis key. In order to do this we can use the `callRaw` function, which will not decode the result as a `JS` `String` and instead will return the result as a `JS` `ArrayBuffer`. Example:

```js
#!js api_version=1.0 name=lib
redis.registerFunction("my_get", 
    (c, key) => {
        return c.callRaw("get", key);
    },
    {
        flags: [redis.functionFalgs.RAW_ARGUMENTS]
    }
);
```

The above example will be able to fetch binary data and return it to the user. For example:

```bash
27.0.0.1:6379> set "\xaa" "\xaa"
OK
127.0.0.1:6379> TFCALL lib.my_get 1 "\xaa"
"\xaa"
```

Notice that a `JS` `ArrayBuffer` can be returned by a function, it will be returned to the client as `bulk string`.

### Binary Keys Names On Database Triggers

On [KeySpace triggers](/docs/interact/programmability/triggers-and-functions/concepts/triggers/keyspace_triggers/), if the key name that triggered the event is binary, the `data.key` field will be NULL. The `data.key_raw` field is always provided as a `JS` `ArrayBuffer` and can be used as in the following example:

```js
#!js api_version=1.0 name=lib
/* The following is just an example, in general it is discourage to use globals. */
var n_notifications = 0;
var last_key = null;
var last_key_raw = null;
redis.registerKeySpaceTrigger("consumer", "", function(client, data) {
    if (data.event == "set") {
        n_notifications += 1;
        last_data = data.key;
        last_key_raw = data.key_raw;
    }
});

redis.registerFunction("notifications_stats", async function(){
    return [
        n_notifications,
        last_key,
        last_key_raw
    ];
});
```

Run example:

```bash
127.0.0.1:6379> set "\xaa" "\xaa"
OK
127.0.0.1:6379> TFCALL lib.notifications_stats 0
1) (integer) 1
2) (nil)
3) "\xaa"
```

For more information see [KeySpace triggers](/docs/interact/programmability/triggers-and-functions/concepts/triggers/keyspace_triggers/).

### Binary Data on Stream Consumers

On [stream triggers](/docs/interact/programmability/triggers-and-functions/concepts/triggers/stream_triggers/), if the key name is binary. The `data.stream_name` field will be NULL. The `data.stream_name_raw` field is always provided as a `JS` `ArrayBuffer` and can be used in this case. In addition, if the content of the steam is binary, it will also appear as `null` under `data.record`. In this case, it is possible to use `data.record` (which always exists) and contains the data as a `JS` `ArrayBuffer`. Example:

```js
#!js api_version=1.0 name=lib
/* The following is just an example, in general it is discourage to use globals. */
var last_key = null;
var last_key_raw = null;
var last_data = null;
var last_data_raw = null;
redis.registerFunction("stats", function(){
    return [
        last_key,
        last_key_raw,
        last_data,
        last_data_raw
    ];
})
redis.registerStreamTrigger("consumer", new Uint8Array([255]).buffer, function(c, data){
    last_key = data.stream_name;
    last_key_raw = data.stream_name_raw;
    last_data = data.record;
    last_data_raw = data.record_raw;
})
```

Run Example:

```bash
127.0.0.1:6379> xadd "\xff\xff" * "\xaa" "\xaa"
"1659515146671-0"
127.0.0.1:6379> TFCALL foo.stats 0
1) (nil)
2) "\xff\xff"
3) 1) 1) (nil)
      2) (nil)
4) 1) 1) "\xaa"
      2) "\xaa"

```
