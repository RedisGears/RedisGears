# Sync and Async Run

By default, each time a function is invoked, it is invoke synchronously. This means that the atomicity property is promised (no other commands will be invoke or Redis while RedisGears function is running). Atomicity property has some great advantages:

* You can update multiple keys at once and be sure any other client will see the entire update (and not partial updates).
* You can be sure the data in Redis are not changed while processing it.

On major disadvantage of the atomicity property is that during the entire invocation Redis is blocked and can not serve any other clients.

Triggers and Functions attempt to give a better flexibility to the function writer and allow to invoke function on the background. When function is invoke on the background it can not touch the Redis key space, To touch the Redis key space from the background, the function must block Redis and enter an atomic section where the atomicity property is once again guaranteed.

Triggers and Functions function can go to the background by implement the function as a JS Coroutine and use `registerAsyncFunction`. The Coroutine is invoked on a background thread and do not block the Redis processes. Example:

```js
#!js api_version=1.0 name=lib

redis.registerAsyncFunction('test', async function(){
    return 'test';
});
```

The above function will simply return `test`, but will run on a background thread and will not block Redis (when running the function, Redis will be able to accept more commands from other clients).

The Coroutine accept an optional client argument, this client is different then the client accepted by synchronous functions. The client does not allow to invoke Redis command, but instead the client allows to block Redis and enter an atomic section where the atomicity property is once again guaranteed. The following example shows how to invoke a simple `ping` command from within an async Coroutine:

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

Notice that this time, in order to invoke the function, we used [`TFCALLASYNC`](commands.md#tfcallasync). **We can only invoke async functions using [`TFCALLASYNC`](commands.md#tfcallasync)**.

Now lets look at a more complex example, assuming we want to write a function that counts the number of hashes in Redis that has name property with some value. Lets first write a synchronous function that does it, we will use the [SCAN](https://redis.io/commands/scan/) command to scan the key space:

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

Though working fine, this function has a potential to block Redis for a long time, lets modify this function to run on the background as a Coroutine:

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

Both implementations return the same result, but the seconds runs in the background and block Redis just to analyze the next batch of keys that returned from the scan command. Other commands will be processed in between the scan batches. Notice that the Coroutine approach allows the key space to be changed while the scanning it, function writer will need to decide if this is acceptable.

# Start Sync and Move Async

The above example is costly, even though Redis is not blocked it is still takes time to return the reply to the user. If we flatten the requirement in such way that we agree to get an approximate value, we can get a much better performance (on most cases). We will cache the result on a key called `<name>_count` and set some expiration on that key so that we will recalculate the value from time to time. The new code will look like this:

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

The above code works as expected, it first check the cache, if cache exists it returns it, otherwise it is perform the calculation and update the cache. But the above example is not optimal, the callback is a Coroutine which means that it will always be calculated on a background thread. Moving to a background thread by itself is costly, the best approach would have been to check the cache synchronously and only if its not there, move to the background. Triggers and Functions allows to start synchronously and move asynchronously using `executeAsync` function. The new code:

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

`executeAsync` will return a `Promise` object, we return this Promise object as the function return value. When Triggers and Functions sees that the function returned a Promise, it waits for the promise to be resolved and return its result to the client. The above implementation will be much faster in case of cache hit.

**Notice** that even though we registered a sync function (not a Coroutine) we still used `registerAsyncFunction`. This is because our function has the potential of blocking the client and take the execution to the background. If we would have used `registerFunction` Triggers and Functions would not have allow us to blocked the client and would have ignore the returned promise object.

**Also notice** it is not always possible to wait for a promise to be resolved, if the command is called inside a `multi/exec` it is not possible to block it and wait for the promise. In such case the client will get an error. It is possible to check if blocking the client is allowed using `client.isBlockAllowed()` function that will return `true` if it is OK to wait for a promise to be resolved and `false` if its not possible.

# Call Blocking Commands

Redis has a few commands that blocks the client and executed asynchronously when some condition holds (commands like [blpop](https://redis.io/commands/blpop/)). In general, such commands are not suppose to be called inside a script and calling them will result in running their none blocking logic. For example, [blpop](https://redis.io/commands/blpop/) will basically runs lpop and return empty result if the list it empty.

RedisGears allows running blocking commands using `client.callAsync` API. `client.callAsync` will execute the blocking command and return a promise object which will be resolved when the command invocation finished (notice that `client.callAsync` allow calling any command and not just blocking but it will always return a promise object that will be resolve later, so **using it for regular commands is less efficient**). 

Example:

```js
#!js api_version=1.0 name=lib

redis.registerAsyncFunction('my_blpop', async function(client, key, expected_val) {
    var res = null
    do {
        res = await client.block((c) => {
            return c.callAsync('blpop', key, '0');
        })
    } while (res[1] != expected_val);
    return res;
});
```

The following function will continue popping elements from the requested list up until it will encounter the requested value. In case the list is empty it will wait until elements will be added to the list.

RedisGears also provided `client.callAsyncRaw` API, which is the same as `client.callAsync` but will not decode the replies as utf8.

**Notice**: There is no guarantee when the promise returned from `client.callAsyn` will be resolved. So the **function writer should not make any assumption about atomicity guarantees.**

# Fail Blocking the Redis

Blocking Redis might fail, couple of reasons for such failure can be:

* Redis reached OOM state and the `redis.functionFlags.NO_WRITES` or `redis.functionFlags.ALLOW_OOM` flags are not set (see [functions flags](function_advance_topics.md#function-flags) for more information)
* `redis.functionFlags.NO_WRITES` flag is not set and the Redis instance turned role and it is now a replica.
* ACL user that invoked the function was deleted.

The failure will result in an exception that the function writer can choose to handle or throw it to be catch by Triggers and Functions.

# Block Redis Timeout

Blocking the Redis for long time is discouraged and considered unsafe operation. Triggers and Functions attempt to protect the function writer and timeout the blocking if it continues for to long. The timeout can be set as a [module configuration](configuration.md) along side the fatal failure policy that indicate how to handle the timeout. Policies can be one of the following:

* Abort - stop the function invocation even at the cost of losing the atomicity property
* Kill - keep the atomicity property and do not stop the function invocation. In such case there is a risk of an external processes to kill the Redis server, thinking that the shard is not responding.
