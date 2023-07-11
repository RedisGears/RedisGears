---
title: "Known limitations"
linkTitle: "Known limitations"
weight: 6
description: >
    Overview of the known limitations
---

## Limited write options

JavaScript remote functions are limited to **read operations** only. Any attempt to perform a write operation of the following functions on a shard different than the one executing the function will result in an error.

- `async_client.runOnShards` runs the remote function on all the shards
- `async_client.runOnKey` runs the remote function on the shard responsible for a given key

In addition, keyspace modification performed by JavaScript functions that are registered using any of the methods available should perform write operations locally:

- If the function is registered with `registerFunction` or `registerAsyncFunction`, it can insert, modify or delete keys that are in the same shard where the function is executed.
- If the function is registered with `registerKeySpaceTrigger` or `registerStreamTrigger`, keyspace modification must be local to the shard that originated the events.

It is also recommended to co-locate the keys to be modified in the same hash slot as the key or Stream that originated the event. As an example, if the user profile stored in the Hash `myserv:user:1234` is subject to changes and we'd like to count them in an external counter, we would name the counter using hash tags: `{myserv:user:1234}:cnt`.

## Exclusive access to the keyspace

By design, asynchronous functions guarantee exclusive single-threaded access to the keyspace, the distinctive feature of Redis. In asynchronous programming with JavaScript functions, access to the keyspace in read or write mode must be blocking, while if not accessing the keyspace, the execution may be non-blocking. This implementation maintains the same level of data consistency as Redis standard commands or Lua scripts and functions but takes advantage of asynchronous execution, a feature of the JavaScript engine.

## JavaScript variables

Not all the JavaScript global variables are made available by the JavaScript engine loaded by Redis (e.g. `console`, `document`). The `redis` global variable can be used to manage functions registration, logging etc. 


