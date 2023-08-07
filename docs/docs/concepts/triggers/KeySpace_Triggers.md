---
title: "KeySpace Triggers"
linkTitle: "KeySpace Triggers"
weight: 2
description: >
    Execute a JavaScript function based on a KeySpace notification
---

KeySpace triggers allow you to register a function that will be executed whenever an event occurs in the database. Most events are triggered by command invocations, but there are two special events that can occur independently of a command:

1. Expired: This event is fired when a key expires from the database.
2. Evicted: This event is fired when a key is evicted from the database.

For a complete list of supported events, please refer to the [Redis Key Space notifications page](https://redis.io/docs/manual/keyspace-notifications/#events-generated-by-different-commands).

To register a KeySpace trigger, you need to use the `redis.registerKeySpaceTrigger` API when loading your library. The following example demonstrates how to register a database trigger that adds a "last updated" field whenever a hash key is modified:

```js
#!js api_version=1.0 name=myFirstLibrary

redis.registerKeySpaceTrigger("consumer", "", function(client, data){
    if (client.call("type", data.key) != "hash") {
        // key is not a has, do not touch it.
        return;
    }
    // get the current time in ms
    var curr_time = client.call("time")[0];
    // set '__last_updated__' with the current time value
    client.call('hset', data.key, '__last_updated__', curr_time);
});
```

Argument Description:

* `consumer`: The consumer name.
* `prefix `: The key prefix on which the trigger should be fired.
* `callback`: The callback function to invoke, following the same rules of [Sync and Async invocation](/docs/interact/programmability/triggers-and-functions/concepts/sync_async/). The callback will only be invoked on the primary shard.

Run the example:

```bash
127.0.0.1:6379> hset h x 1
(integer) 1
127.0.0.1:6379> hgetall h
1) "x"
2) "1"
3) "__last_updated__"
4) "1658390831"
127.0.0.1:6379> hincrby h x 1
(integer) 2
127.0.0.1:6379> hgetall h
1) "x"
2) "2"
3) "__last_updated__"
4) "1658390910"
```

The `data` argument, which is passed to the consumer callback, are in the following format:

```json
{
    "event": "<the event name that fired the trigger>",
    "key": "<key name that the event was fired on as String>",
    "key_raw": "<key name that the event was fired on as ArrayBuffer>"
}
```

Notice that the `key` field is given only if the key can be decoded as a `JS` `String`, otherwise the value will be `null`.

We can display trigger information using `TFUNCTION LIST` command:

```bash
127.0.0.1:6379> TFUNCTION list vvv
1)  1) "engine"
    2) "js"
    3) "api_version"
    4) "1.0"
    5) "name"
    6) "foo"
    7) "pending_jobs"
    8) (integer) 0
    9) "user"
    10) "default"
    11) "functions"
   12) (empty array)
   13) "keyspace_triggers"
   14) (empty array)
   15) "stream_triggers"
   16) 1)  1) "name"
           2) "consumer"
           3) "num_triggered"
           4) (integer) 2
           5) "num_finished"
           6) (integer) 2
           7) "num_success"
           8) (integer) 1
           9) "num_failed"
          10) (integer) 1
          11) "last_error"
          12) "TypeError: redis.call is not a function"
          13) "last_exection_time"
          14) (integer) 0
          15) "total_exection_time"
          16) (integer) 0
          17) "avg_exection_time"
          18) "0"
```

## Triggers Guarantees

If the callback function passed to the trigger is a `JS` function (not a Coroutine), it is guaranteed that the callback will be invoked atomically along side the operation that caused the trigger; meaning all clients will see the data only after the callback has completed. In addition, it is guaranteed that the effect of the callback will be replicated to the replica and the AOF in a `multi/exec` block together with the command that fired the trigger.

If the callback is a Coroutine, it will be executed in the background and there is no guarantee on where or if it will be executed. The guarantees are the same as described on [Sync and Async invocation](/docs/interact/programmability/triggers-and-functions/concepts/sync_async/).

## Upgrades

When upgrading existing trigger code using the `REPLACE` option of `TFUNCTION LOAD` command, all trigger parameters can be modified.

## Advanced Usage

For most use cases, the `registerKeySpaceTrigger` API is sufficient. But there are some use cases where you might need a better guarantee on when a trigger will be fired. Let's look at the following example:

```js
#!js api_version=1.0 name=myFirstLibrary

redis.registerKeySpaceTrigger("consumer", "", function(client, data){
    if (client.call("type", data.key) != "hash") {
        // key is not a has, do not touch it.
        return;
    }
    var name = client.call('hget', data.key, 'name');
    client.call('incr', `name_${name}`);
});
```

Whenever a hash key is changed, the example above will read the field `name` from the hash, for example `tom`, and increase the value of the key `name_tom`. (**Notice: This function will not work properly on cluster. We need to use `{}` on the key's name to make sure we are writing to a key located on the current shard. For the sake of simplicity we will ignore the cluster issues for the time being**). Running the function will give the following results:

```bash
127.0.0.1:6379> hset x name tom
(integer) 1
127.0.0.1:6379> hset x name jerry
(integer) 0
127.0.0.1:6379> get name_tom
"1"
127.0.0.1:6379> get name_jerry
"1"
```

We can see that the key `name_tom` was incremented once, and the key `name_jerry` was incremented once. Will we get the same results if we wrap the `hset` call with a `multi`/`exec`?

```bash
127.0.0.1:6379> multi
OK
127.0.0.1:6379(TX)> hset x name tom
QUEUED
127.0.0.1:6379(TX)> hset x name jerry
QUEUED
127.0.0.1:6379(TX)> exec
1) (integer) 1
2) (integer) 0
127.0.0.1:6379> get name_tom
(nil)
127.0.0.1:6379> get name_jerry
"2"
```

What just happened? `name_jerry` was incremented twice while `name_tom` was not incremented at all. This happened because, in case of a `multi`/`exec` or Lua function, the notifications are fired at the end of the transaction, so all the clients will receive notifications of the last value written, which is `jerry`.

To fix the code and still get the expected results even on `multi`/`exec`. Triggers and functions allows you to specify an optional callback that will run exactly when the notification happened (and not at the end of the transaction). The constraint on this callback is that it can only **read** data without performing any writes. The new code will be as follow:

```js
#!js api_version=1.0 name=lib

redis.registerKeySpaceTrigger("consumer", "", function(client, data){
    if (data.name !== undefined) {
        client.call('incr', `name_${data.name}`);
    }
},{
    onTriggerFired: (client, data) => {
        if (client.call("type", data.key) != "hash") {
            // key is not a has, do not touch it.
            return;
        }
        data.name = client.call('hget', data.key, 'name');
    }
});
```

The above code gives an optional function argument, `onTriggerFired`, to our trigger. The function will be fired right after a key change and will allow us to read the content of the key. We are adding the content into the `data` argument, which will be given to the actual trigger function that can write the data. The above code works as expected:

```bash
127.0.0.1:6379> multi
OK
127.0.0.1:6379(TX)> hset x name tom
QUEUED
127.0.0.1:6379(TX)> hset x name jerry
QUEUED
127.0.0.1:6379(TX)> exec
1) (integer) 1
2) (integer) 0
127.0.0.1:6379> get name_tom
"1"
127.0.0.1:6379> get name_jerry
"1"
```
