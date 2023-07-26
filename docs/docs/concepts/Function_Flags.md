---
title: "Function flags"
linkTitle: "Function flags"
weight: 5
description: >
    Function flags for JavaScript functions
---

When registering a function, it is possible to include additional information about its behavior. This information is known as function flags. Function flags are an optional argument that can be specified after the function implementation. The following flags are supported:
1. `redis.functionFlags.NO_WRITES`: This flag indicates that the function does not perform any write commands. Enabling this flag allows a function to be executed on read-only replicas or in out-of-memory (OOM) situations. Redis enforces this flag's behavior, meaning that any attempt to call a write command within a function that has this flag set will result in an exception.
2. `redis.functionFlags.ALLOW_OOM`: By default, Redis prevents any function from running in an OOM scenario. However, this flag allows overriding this behavior and running a function even when there is a memory shortage. Enabling this flag is considered unsafe and may cause Redis to exceed the `maxmemory` limit. **Users should only enable this flag if they are certain that their function does not consume additional memory.** For example, it is safe to run a function that only deletes data during an OOM situation.
3. `redis.functionFlags.RAW_ARGUMENTS`: By default, Redis attempts to decode all function arguments as `JS` `String`s. If the decoding fails, an error is returned to the client. However, when this flag is set, Redis avoids string decoding and passes the argument as a `JS` `ArrayBuffer` instead.

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
