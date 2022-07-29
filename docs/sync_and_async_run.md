# Sync and Async Run

By default, each time a RedisGears function is invoked, it is invoked synchronously. This means that the atomicity property is promised (no other commands will be invoked while a RedisGears function is running). Atomicity property has some great adventages:

* You can update multiple keys at once and be certain all clients will see the entire update (and no partial updates).
* You can be sure that the data in Redis is not changed while processing it.

One major disadventage of the atomicity property is that during the entire invocation Redis is blocked and can not serve any other clients.

RedisGears attempt to give better flexibility to the Gears function writer and allows to invoke functions in the background. When a function is invoked in the background it can not touch the Redis key space, To touch the Redis key space from the background, the function must block Redis and enter an atomic section where the atomicity property is once again guaranteed.

RedisGears function can go to the background by implementing the function as a JS coroutine. The coroutine is invoked on a background thread and does not block the Redis process. Example:

```js
#!js name=lib

redis.register_function('test', async function(){
    return 'test';
});
```

The above function will simply return `test`, but will run on a background thread and will not block Redis (when running the function, Redis will be able to accept more commands from other clients).

The coroutine accept an optional client argument, this client is different then the client accepted by synchronous functions. The client does not allow the invoking of a Redis command, but instead the client allows to block Redis and enter an atomic section where the atomicity propert is once again guaranteed. The following example shows how to invoke a simple `ping` command from within an async coroutine:

```js
#!js name=lib

redis.register_function('test', async function(client){
    return client.block(function(redis_client){
        return redis_client.call('ping');
    });
});
```

Running this function will return a `pong` reply:

```bash
127.0.0.1:6379> RG.FCALL lib test 0
"PONG"
```

Lets look at a more complex example, assuming we want to write a function that counts the number of hashes in Redis that has name property with some value. Lets first write a synchronous function that does it, we will use the [SCAN](https://redis.io/commands/scan/) command to scan the key space:

```js
#!js name=lib

redis.register_function('test', function(client, expected_name){
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

Though working fine, this function has the potential to block Redis for an extended time, lets modify this function to run in the background as a coroutine:

```js
#!js name=lib

redis.register_function('test', async function(async_client, expected_name){
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

Both implementations return the same result, but the second runs in the background and blocks Redis just to analize the next batch of keys that returned from the scan command. Other commands will be processed in between the scan batches. Notice that the coroutine approach allows the key space to be changed while scanning it, the function writer will need to decide if this is acceptable.

# Start Sync and Move Async

The above example is costly, even though Redis is not blocked it still takes time to return the reply to the user. If we flatten the requirement in such a way that we agree to get an approximate value, we can get a much better performance (in most cases). We will cache the result on a key called `<name>_count` and set some expiration on that key so that we will recalculate the value from time to time. The new code will look like this:

```js
#!js name=lib

redis.register_function('test', async function(async_client, expected_name){
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

The above code works as expected, it first check the cache, if cache exists it returns it, otherwise it performs the calculation and updates the cache. But the above example is not optimal, the callback is a coroutine which means that it will always be calculated on a background thread. Moving to a background thread by itself is costly, the best approach would have been to check the cache synchronously and only if its not there, move to the background. RedisGears allows to start synchronously and move asynchronously using `run_on_background` function. The new code:

```js
#!js name=lib

redis.register_function('test', function(client, expected_name){
    // check the cache first
    var cached_value = client.call('get', expected_name + '_count');
    if (cached_value != null) {
        return cached_value;
    }

    // cache is not set, move to background
    return client.run_on_background(async function(async_client) {
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

`run_on_background` will return a `Promise` object, we return this Promise object as the function return value. When RedisGears sees that the function returned a Promise, it waits for the promise to be resolved and returns the result to the client. The above implementation will be much faster in case of cache hit.

**Notice!!!** it is not always possible to wait for a promise to be resolved, if the command is called inside a `multi/exec` it is not possible to block it and wait for the promise. In such case the client will get an error. It is possible to check if blocking the client is allowed using `client.allow_block()` function that will return `true` if it is OK to wait for a promise to be resolved and `false` if its not possibe.


# Fail Blocking the Redis

Blocking Redis might fail, couple of reasons for such failure can be:

* Redis reached OOM state and the `no-writes` or `allow-oom` flags are not set (see [functions flags](function_advance_topics.md#function-flags) for more information)
* `no-writes` flag is not set and the Redis instance turned role and it is now a replica.
* ACL user that invoked the function was deleted.

The failure will result in an exception that the function writer can choose to handle or throw it to be catched by RedisGears.

# Block Redis Timeout

Blocking the Redis for long time is discouraged and considered unsafe operation. RedisGears attempts to protect the function writer and times out the blocking if it continues for to long. The timeout can be set as a [module configuration](configuration.md) along side the fatal failure policy that indicate how to handle the timeout. Policies can be one of the following:

* Abort - stop the function invocation even at the cost of losing the atomicity property
* Kill - keep the atomicity property and do not stop the function invocation. In such cases there is a risk of an external process killing the Redis server, thinking that the shard is not responding.
