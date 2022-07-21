# Database Triggers

Database triggers allow register a function that will be invoked wheneven an event happened on the database. Most of the events triggers by command invocation but there are 2 special event that is not necessarally triggered by a command:

1. Expired - fired when a key expired from the database.
2. Evicted - fired when a key evicted from the database.

For the full list of supported events please refer to [Redis Key Space notifications page](https://redis.io/docs/manual/keyspace-notifications/#events-generated-by-different-commands)

To register a database trigger we need to use the `redis.register_notifications_consumer` API when loading our library. The following example shows how to register a database trigger that will add a last update field when ever a hash key is changed:

```js
#!js name=lib

redis.register_notifications_consumer("consumer", "", function(client, data){
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

Argument Discription:

* consumer - the consumer name.
* prefix - the prefix of keys we want to fire the trigger on
* callback - the callback to invoke. Following the same rules of [Sync and Async invocation](sync_and_async_run.md). The callback will be invoke only on primary shard.

Run example:

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

The `data` argument which pass to the consumer callback are in the following format:

```json
{
    "event": "<the event name that fired the trigger>",
    "key": "<key name that the event was fired on>",
}
```

We can observe the trigger information using [RG.FUNCTION LIST](commands.md#rgfunction-list) command:

```bash
127.0.0.1:6379> RG.FUNCTION list vvv
1)  1) "engine"
    2) "js"
    3) "name"
    4) "foo"
    5) "pending_jobs"
    6) (integer) 0
    7) "user"
    8) "default"
    9) "functions"
   10) (empty array)
   11) "stream_consumers"
   12) (empty array)
   13) "notifications_consumers"
   14) 1)  1) "name"
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
   15) "gears_box_info"
   16) (nil)
```

## Triggers Guarantees

If the callback pass to the trigger is a `JS` function (not a Coroutine), it is guarantee that the callback will be invoke atomically along side the operation that cause the trigger, i.e all client will see the data only after the callback has finished. In addition, it is guarantee that the effect of the callback will be replicated to the replication and the AOF in a `multi/exec` block together with the command that fired the trigger.

If the callback is a Coroutine, it will be executed in the background and there is not guarantees on where or if it will be executed. The guarantees are the same as describe on [Sync and Async invocation](sync_and_async_run.md).

## Upgrades

When upgrading the trigger code (using the `UPGRADE` option of [`RG.FUNCTION LOAD`](commands.md#rgfunction-load) command) all the trigger parameters can be modified.

## Known Issues

On the current Redis version (7.0.3) there are couple of known issues that effects databases triggers:

* The effect of the trigger and the command that fire the trigger will be replicated (to the replica and AOF) at a reverse order. This means that if `set x 1` fire a trigger that perfroms `del x`, the replication will see the `del x` command before the `set x 1` command. This will cause replication inconsistency. To avoid it, the trigger should only perform operations that are indipendent and are not effected the by execution order.
* On active expire, the `del` command and the trigger effect will not be wrapped with `multi/exec` block.
* On active eviction, the `del` command and the trigger effect will not be wrapped with `multi/exec` block.

There is already a [PR](https://github.com/redis/redis/pull/10969) that should fix those issues on Redis, we hope it will be merged soon.
