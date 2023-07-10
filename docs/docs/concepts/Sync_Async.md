---
title: "Sync & Async"
linkTitle: "Sync & Async"
weight: 3
description: >
    Sync and Async Functions
---

By default, each time a function is invoked, it is executed synchronously. This ensures the atomicity property, meaning that no other commands will be executed on Redis while the function is running. The atomicity property offers several advantages:

* Multiple keys can be updated simultaneously, guaranteeing that other clients see the complete update rather than partial updates.
* The data in Redis remains unchanged while it is being processed.

However, the major disadvantage of the atomicity property is that Redis is blocked throughout the entire invocation, preventing it from serving other clients.

Triggers and Functions aim to provide greater flexibility to function writers by enabling the invocation of functions in the background. When a function is invoked in the background, it cannot directly access the Redis key space. To interact with the Redis key space from the background, the function must block Redis and enter an atomic section where the atomicity property is once again guaranteed.

To run Triggers and Functions in the background, functions can be implemented as JS coroutines using the `registerAsyncFunction` API. The coroutine is invoked on a background thread and does not block the Redis processes. Here's an example:

```js
#!js api_version=1.0 name=lib

redis.registerAsyncFunction('test', async function(){
    return 'test';
});
```

The simple function shown above will return the value 'test' and will execute on a background thread without blocking Redis. This allows Redis to continue accepting commands from other clients while the function is running.

The coroutine also accepts an optional client argument, which differs from the client used in synchronous functions. This client argument does not allow direct invocation of Redis commands. Instead, it provides the capability to block Redis and enter an atomic section where the atomicity property is once again guaranteed. Here's an example that demonstrates invoking a ping command from within an async coroutine:

```js
#!js api_version=1.0 name=lib

redis.registerFunction('test', async function(client){
    return client.block(function(redis_client){
        return redis_client.call('ping');
    });
});
```

Running this function will return a `pong` reply:

```bash
127.0.0.1:6379> TFCALLASYNC lib.test 0
"PONG"
```

Notice that this time, in order to invoke the function, we used `TFCALLASYNC`. **We can only invoke async functions using `TFCALLASYNC`**.

Now let's look at a more complex example. Assume we want to write a function that counts the number of hashes in Redis that have a `name` property with some value. As a first attempt, we'll write a synchronous function that uses the `SCAN` command to scan the key space:

```js
#!js api_version=1.0 name=lib

redis.registerFunction('test', function(client, expected_name){
    var count = 0;
    var cursor = '0';
    do{
        var res = client.call('scan', cursor);
        cursor = res[0];
        var keys = res[1];
        keys.forEach((key) => {
            if (client.call('hget', key, 'name') == expected_name) {
                count += 1;
            }
        });
    } while(cursor != '0');
    return count;
});
```

While this function works, it has the potential to block Redis for a long time. So let's modify this function to run in the background as a coroutine:

```js
#!js api_version=1.0 name=lib

redis.registerAsyncFunction('test', async function(async_client, expected_name){
    var count = 0;
    var cursor = '0';
    do{
        async_client.block((client)=>{
            var res = client.call('scan', cursor);
            cursor = res[0];
            var keys = res[1];
            keys.forEach((key) => {
                if (client.call('hget', key, 'name') == expected_name) {
                    count += 1;
                }
            });
        });
    } while(cursor != '0');
    return count;
});
```

Both implementations return the same result, but the second function runs in the background and blocks Redis just to analyze the next batch of keys that are returned from the `SCAN` command. Other commands will be processed in between `SCAN` batches. Notice that the coroutine approach allows the key space to be changed while the scanning it. The function writer will need to decide if this is acceptable.

# Start Sync and Move Async

The previous example, although functional, has a drawback in terms of performance. Even though Redis is not blocked, it still takes time to return the reply to the user. However, if we modify the requirement slightly and agree to obtain an approximate value, we can achieve much better performance in most cases. This can be done by implementing result caching using a key named `<name>_count` and setting an expiration time on that key, which triggers recalculation of the value periodically. Here's the updated code:

```js
#!js api_version=1.0 name=lib

redis.registerAsyncFunction('test', async function(async_client, expected_name){
    // check the cache first
    var cached_value = async_client.block((client)=>{
        return client.call('get', expected_name + '_count');
    });

    if (cached_value != null) {
        return cached_value;
    }


    var count = 0;
    var cursor = '0';
    do{
        async_client.block((client)=>{
            var res = client.call('scan', cursor);
            cursor = res[0];
            var keys = res[1];
            keys.forEach((key) => {
                if (client.call('hget', key, 'name') == expected_name) {
                    count += 1;
                }
            });
        });
    } while(cursor != '0');

    // set count to the cache wil 5 seconds expiration
    async_client.block((client)=>{
        client.call('set', expected_name + '_count', count);
        client.call('expire', expected_name + '_count', 5);
    });

    return count;
});
```

The above code works as expected. It first checks the cache and if the cache exists it's returned. Otherwise it will perform the calculation and update the cache. But the above example is not optimal. The callback is a coroutine, which means that it will always be calculated on a background thread. Intrinsically, moving to a background thread is costly. The best approach would be to check the cache synchronously and, only if its not there, move to the background. Triggers and Functions provides for starting synchronously and then moving asynchronously using `executeAsync` function as required. The new code:

```js
#!js api_version=1.0 name=lib

redis.registerAsyncFunction('test', function(client, expected_name){
    // check the cache first
    var cached_value = client.call('get', expected_name + '_count');
    if (cached_value != null) {
        return cached_value;
    }

    // cache is not set, move to background
    return client.executeAsync(async function(async_client) {
        var count = 0;
        var cursor = '0';
        do{
            async_client.block((client)=>{
                var res = client.call('scan', cursor);
                cursor = res[0];
                var keys = res[1];
                keys.forEach((key) => {
                    if (client.call('hget', key, 'name') == expected_name) {
                        count += 1;
                    }
                });
            });
        } while(cursor != '0');

        // set count to the cache wil 5 seconds expiration
        async_client.block((client)=>{
            client.call('set', expected_name + '_count', count);
            client.call('expire', expected_name + '_count', 5);
        });

        return count;
    });
});
```

`executeAsync` will return a `Promise` object. When Triggers and Functions sees that the function returns a Promise, it waits for the promise to be resolved and returns its result to the client. The above implementation will be much faster in the case of cache hit.

**Notice** that even though we registered a synchronous function (not a coroutine) we still used `registerAsyncFunction`. This is because our function has the potential of blocking the client, taking the execution to the background. If we had used `registerFunction`, Triggers and Functions would not have allowed the function to block the client and it would have ignored the returned promise object.

**Also notice** it is not always possible to wait for a promise to be resolved. If the command is called inside a `multi/exec` it is not possible to block it and wait for the promise. In such cases the client will get an error. It is possible to check if blocking the client is allowed using the `client.isBlockAllowed()` function, which will return `true` if it is OK to wait for a promise to be resolved and `false` if it is not possible.


# Fail Blocking the Redis

Blocking Redis might fail for a few reasons:

* Redis reached OOM state and the `redis.functionFlags.NO_WRITES` or `redis.functionFlags.ALLOW_OOM` flags are not set (see [functions flags](./Function_Flags.md) for more information)
* `redis.functionFlags.NO_WRITES` flag is not set and the Redis instance changed roles and is now a replica.
* The ACL user that invoked the function was deleted.

The failure will result in an exception that the function writer can choose to handle or throw it to be caught by Triggers and Functions.

# Block Redis Timeout

Blocking Redis for a long time is discouraged and is considered an unsafe operation. Triggers and Functions attempts to protect the function writer and will time out the blocking function if it continues for too long. The timeout can be set as a [module configuration](./../Configuration.md) along side the fatal failure policy that indicates how to handle the timeout. Policies can be one of the following:

* Abort - Stop the function invocation even at the cost of losing the atomicity property.
* Kill - Keep the atomicity property and do not stop the function invocation. In this case there is a risk of an external process killing the Redis server, thinking that the shard is not responding.
