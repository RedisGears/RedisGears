---
title: "Function flags"
linkTitle: "Function flags"
weight: 5
description: >
    Function flags for JavaScript functions
---

It is possible to provide some information about the function behaviour on registration time. Such information is called function flags. The function flags is a optional argument that can be given after the function implementation. The supported flags are:
1. `redis.functionFlags.NO_WRITES` - indicating that the function performs not write commands. If this flag is on, it will be possible to run the function on read only replicas or on OOM. RedisGears force this flag behaviour, this means that any attempt to call a write command from within a function that has this flag will result in an exception.
2. `redis.functionFlags.ALLOW_OOM` - by default, RedisGears will not allow running any function on OOM. This flag allows overide this behaviour and running the function even on OOM. Enable this flag is considered unsafe and could cause Redis to bypass the maxmemory value. **User should only enable this flag if he knows for sure that his function do not consume memory** (for example, on OOM, it is safe to run a function that only deletes data).
3. `redis.functionFlags.RAW_ARGUMENTS` - by default, RedisGears will try to decode all function arguments as `JS` `String` and if it failed an error will be return to the client. When this flag is set, RedisGears will avoid String decoding and will pass the argument as `JS` `ArrayBuffer`.

The following example shows how to set the `redis.functionFlags.NO_WRITES` flag:

```js
#!js api_version=1.0 name=lib

redis.registerFunction('my_ping',
    function(client){
        return client.call('ping');
    },
    {
        flags: [redis.functionFlags.NO_WRITES]
    }
);
```

Run example:

```bash
127.0.0.1:6379> TFCALL lib.my_ping 0
"PONG"
127.0.0.1:6379> config set maxmemory 1
OK
127.0.0.1:6379> TFCALL lib.my_ping 0
"PONG"

```
