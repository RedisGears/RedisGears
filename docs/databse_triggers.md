# Database Triggers

Database triggers allow register a function that will be invoked whenever an event happened on the database. Most of the events triggers by command invocation but there are 2 special events that are not necessarily triggered by a command:

1. Expired - fired when a key expired from the database.
2. Evicted - fired when a key evicted from the database.

For the full list of supported events please refer to [Redis Key Space notifications page](https://redis.io/docs/manual/keyspace-notifications/#events-generated-by-different-commands)

To register a database trigger we need to use the `redis.register_notifications_consumer` API when loading our library. The following example shows how to register a database trigger that will add a last update field when ever a hash key is changed:

```js
#!js api_version=1.0 name=lib

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

Argument Description:

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
    "key": "<key name that the event was fired on as String>",
    "key_raw": "<key name that the event was fired on as ArrayBuffer>"
}
```

Notice that `key` field is given only if the key can be decoded as `String`, otherwise the value will be `null`.

We can observe the trigger information using [RG.FUNCTION LIST](commands.md#rgfunction-list) command:

```bash
127.0.0.1:6379> RG.FUNCTION list vvv
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
   13) "stream_consumers"
   14) (empty array)
   15) "notifications_consumers"
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
   17) "gears_box_info"
   18) (nil)
```

## Triggers Guarantees

If the callback pass to the trigger is a `JS` function (not a Coroutine), it is guarantee that the callback will be invoke atomically along side the operation that cause the trigger, i.e all client will see the data only after the callback has finished. In addition, it is guarantee that the effect of the callback will be replicated to the replication and the AOF in a `multi/exec` block together with the command that fired the trigger.

If the callback is a Coroutine, it will be executed in the background and there is not guarantees on where or if it will be executed. The guarantees are the same as describe on [Sync and Async invocation](sync_and_async_run.md).

## Upgrades

When upgrading the trigger code (using the `UPGRADE` option of [`RG.FUNCTION LOAD`](commands.md#rgfunction-load) command) all the trigger parameters can be modified.

## Advance Usage

For most use cases, `register_notifications_consumer` API is enough. But there are some use cases where you might need a better guaranteed on when the trigger will be fired. Lets look at the following example:

```js
#!js api_version=1.0 name=lib

redis.register_notifications_consumer("consumer", "", function(client, data){
    if (client.call("type", data.key) != "hash") {
        // key is not a has, do not touch it.
        return;
    }
    var name = client.call('hget', data.key, 'name');
    client.call('incr', `name_${name}`);
});
```

Whenever a hash key is changing, the example above will read the field `name` from the hash (assume its `foo`) and increase the value of the key `name_foo` (**notice that this function will not work properly on cluster, we need to use `{}` on the keys name to make sure we are writing to a key located on the current shard, for simplicity we ignore the cluster issues now**). Running the function will give the following results:

```bash
127.0.0.1:6379> hset x name foo
(integer) 1
127.0.0.1:6379> hset x name bar
(integer) 0
127.0.0.1:6379> get name_foo
"1"
127.0.0.1:6379> get name_bar
"1"
```

We can see that the key `name_foo` was increased once, and the key `name_bar` was increased once. Will we get the same results if we wrap the `hset`'s in a `multi`/`exec`?

```bash
127.0.0.1:6379> multi
OK
127.0.0.1:6379(TX)> hset x name foo
QUEUED
127.0.0.1:6379(TX)> hset x name bar
QUEUED
127.0.0.1:6379(TX)> exec
1) (integer) 1
2) (integer) 0
127.0.0.1:6379> get name_foo
(nil)
127.0.0.1:6379> get name_bar
"2"
```

What just happened? `name_bar` was increased once while `name_foo` was not increased at all. This happened because in case of a `multi`/`exec` or Lua, the notifications are fire at the end of the transaction, so all the notifications will see the last value that was written which is `bar`.

To fix the code and still get the expected results even on `multi`/`exec`. RedisGears allow to specify an optional callback that will run exactly when the notification happened (and not at the end of the transaction). The constrains on this callback is that it can only **read** data without performing any writes. The new code will be as follow:

```js
#!js api_version=1.0 name=lib

redis.register_notifications_consumer("consumer", "", function(client, data){
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

The above code gives an optional function argument, `onTriggerFired`, to our trigger. The function will be fired right after the key change and will allow us to read the content of the key. We are adding the content into the `data` argument which will be given to the actual trigger function that can write the data. The above code works as expected:

```bash
127.0.0.1:6379> multi
OK
127.0.0.1:6379(TX)> hset x name foo
QUEUED
127.0.0.1:6379(TX)> hset x name bar
QUEUED
127.0.0.1:6379(TX)> exec
1) (integer) 1
2) (integer) 0
127.0.0.1:6379> get name_foo
"1"
127.0.0.1:6379> get name_bar
"1"
```
